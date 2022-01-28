(ns qot.clj-zyte-api
  (:require [org.httpkit.client :as ohttp]
            [qot.clj-zyte-api :as api]
            [qot.clj-zyte-api.utils :as utils]
            ;; [clojure.spec.alpha :as s]
            [clojure.string :as str]
            [cheshire.core :as json])
  ;; (:require [com.fulcrologic.guardrails.core :refer [>defn >def >fdef | ? =>]]
)

(declare ZyteHcf)

;; (>def :zyte/project-id (s/and string? #(re-matches #"\d+" %)))
;; (>def :zyte/project-coordinates (s/keys :opt-un [:zyte/project-id]))
;; (>def :zyte/client #(extends? ZyteHcf %))

;; (>def :zyte-frontier/frontier string?)
;; (>def :zyte-frontier/slot string?)
;; (>def :zyte-frontier/frontier-coordinates
;;       (s/merge
;;        :zyte/project-coordinates
;;        (s/keys :req-un [:zyte-frontier/frontier])))
;; (>def :zyte-frontier/slot-coordinates
;;       (s/merge :zyte-frontier/frontier-coordinates
;;                (s/keys :req-un [:zyte-frontier/slot])))

;; (>def :zyte-frontier/requests-added pos-int?)
;; (>def :zyte-frontier/requests-return (s/keys :req-un [:zyte-frontier/requests-added]))

;; (>def :zyte-frontier/fingerprint string?)
;; (>def :zyte-frontier/queue-data map?)
;; (>def :zyte-frontier/fingerprint-data map?)
;; (>def :zyte-frontier/priority nat-int?)


;; (>def :zyte-frontier/request (s/keys :req-un [:zyte-frontier/fingerprint]
;;                                      :opt-un [:zyte-frontier/queue-data
;;                                               :zyte-frontier/fingerprint-data
;;                                               :zyte-frontier/priority]))

;; (>fdef hcf-add-requests
;;        [impl coordinates requests]
;;        [:zyte/client :zyte-frontier/slot-coordinates (s/coll-of :zyte-frontier/requestx)
;;         => :zyte-frontier/requests-return])

;; (>fdef hcf-delete-slot
;;        [impl coordinates]
;;        [:zyte/client :zyte-frontier/slot-coordinates => int?])

(def zyte-storage-root "https://storage.scrapinghub.com")

(defn parse-json-lines
  [s]
  (->> (str/split s #"\n")
       (map #(json/decode % true))))

(defn send-request
  [{:keys [api-key http-client] :as client} {:keys [url] ::keys [no-retry?] :as request}]
  (let [full-request (merge {:headers {"accept" "application/json"}
                             :basic-auth (str api-key ":")
                             :client @http-client
                             :as :text}
                            request)
        {:keys [status body error] :as result} @(ohttp/request full-request)]
    (cond
      (and error
           (not no-retry?)
           (= (ex-message error) "Cannot kickstart, the connection is broken or closed"))
      (do
        (reset! client (ohttp/make-client {}))
        (send-request client (assoc request ::no-retry? true)))
      (and (not error) (re-find #"^2" (str status)))
      (cond
        (= :json (:coerce request)) (json/decode (:body result) true)
        (= :json-lines (:coerce request)) (parse-json-lines (:body result))
        :else (:body result))
      :else
      (throw (ex-info (format "Error calling Zyte API at %s Code [%s]" url status)
                      {:url url
                       :status status
                       :error error})))))

;; (s/fdef make-hcf-path
;;   :args (s/cat :coordinates (s/or :slot-coordinates :zyte-frontier/slot-coordinates
;;                                   :frontier-coordinates :zyte-frontier/frontier-coordinates
;;                                   :project-coordinates :zyte/project-coordinates))
;;   :ret string?)

(defn make-coll-path
  [{:keys [project-id collection type] :as coords}]
  (cond (and project-id collection type) (format "/collections/%s/%s/%s" project-id (name type) (name collection))
        project-id (format "/collections/%s" project-id)
        :else (throw (ex-info "Insufficient Collection API coordinates given" {:coordinates coords}))))

(defn make-hcf-path
  [{:keys [project-id frontier slot] :as coords}]
  (cond (and project-id frontier slot) (format "/hcf/%s/%s/s/%s" project-id frontier slot)
        (and project-id frontier) (format "/hcf/%s/%s" project-id frontier)
        project-id (format "/hcf/%s" project-id)
        :else (throw (ex-info "Insufficient Frontier API coordinates given" {:coordinates coords}))))

(defn hcf-add-requests
  [{:keys [project-id] :as client} coords requests]
  (let [full-coords (utils/assoc-default-val coords :project-id project-id)
        url (str zyte-storage-root (make-hcf-path full-coords))
        lines (for [{:keys [queue-data fingerprint-data priority fingerprint]} requests]
                (-> {:fp fingerprint}
                    (cond-> queue-data (assoc :qdata queue-data))
                    (cond-> fingerprint-data (assoc :fdata fingerprint-data))
                    (cond-> priority (assoc :p priority))
                    (json/encode)))
        req {:url url
             :method :post
             :coerce :json
             :body (str/join "\n" lines)
             :content-type :json}
        {:keys [newcount]} (send-request client req)]
    {:requests-added newcount}))

(defn hcf-delete-slot
 [{:keys [project-id] :as client} coordinates]
 (let [full-coords (utils/assoc-default-val coordinates :project-id project-id)
       url (str zyte-storage-root (make-hcf-path full-coords))]
   (send-request client
                 {:url url
                  :method :delete
                  :coerce :json-lines})
   true))

(defn hcf-get-batch-requests
 [{:keys [project-id] :as client} coordinates opts]
 (let [{:keys [limit]} opts
       full-coords (utils/assoc-default-val coordinates :project-id project-id)
       url (str zyte-storage-root (make-hcf-path full-coords) "/q")
       results (send-request client
                             {:url url
                              :method :get
                              :query-params (when limit {:mincount limit})
                              :coerce :json-lines})]
   (for [{:keys [id requests]} results]
     {:batch-id id :requests (map (fn [[fp data]] (-> {:fingerprint fp}
                                                      (cond-> data (assoc :queue-data data))))
                                  requests)})))
(defn hcf-list-fingerprints
 [{:keys [project-id] :as client} coordinates]
 (let [full-coords (utils/assoc-default-val coordinates :project-id project-id)
       url (str zyte-storage-root (make-hcf-path full-coords) "/f")
       fps (send-request client
                         {:url url
                          :method :get
                          :coerce :json-lines})]
   (for [{:keys [fp fdata]} fps]
     (-> {:fingerprint fp}
         (cond-> fdata (assoc :fingerprint-data fdata))))))

(defn hcf-delete-batch-requests
  [{:keys [project-id] :as client} coordinates ids]
 (let [full-coords (utils/assoc-default-val coordinates :project-id project-id)
       url (str zyte-storage-root (make-hcf-path full-coords) "/q/deleted")
       body (->> ids
                 (map name)
                 (map json/encode)
                 (str/join "\n"))]
   (send-request client
                 {:url url
                  :method :post
                  :body body
                  :coerce :json-lines})
   true))

(defn hcf-list-slots
  [{:keys [project-id] :as client} coordinates]
 (let [full-coords (utils/assoc-default-val coordinates :project-id project-id)
       url (str zyte-storage-root (make-hcf-path full-coords) "/list")
       res (send-request client
                         {:url url
                          :coerce :json
                          :method :get})]
   res))
(defn hcf-list-frontiers
  [{:keys [project-id] :as client} coordinates]
 (let [full-coords (utils/assoc-default-val coordinates :project-id project-id)
       url (str zyte-storage-root (make-hcf-path full-coords) "/list")
       res (send-request client
                         {:url url
                          :coerce :json
                          :method :get})]
   res))

(defn coll-upsert-records
  [{:keys [project-id] :as client} coll data]
 (let [full-coll (utils/assoc-default-val coll :project-id project-id)
       url (str zyte-storage-root (make-coll-path full-coll))
       body (->> data
                 (map json/encode)
                 (str/join "\n"))]
   (send-request client
                 {:url url
                  :body body
                  :method :post})
   true))

(defn coll-get-record
  [{:keys [project-id] :as client} coll k]
 (let [full-coll (utils/assoc-default-val coll :project-id project-id)
       url (str zyte-storage-root (make-coll-path full-coll) "/" k)]
   (first (send-request client
                        {:url url
                         :method :get
                         :coerce :json}))))

(defn coll-delete-record
  [{:keys [project-id] :as client} coll k]
 (let [full-coll (utils/assoc-default-val coll :project-id project-id)
       url (str zyte-storage-root (make-coll-path full-coll) "/" k)]
   (send-request client
                 {:url url
                  :method :delete
                  :coerce :json})
   true))

(defn coll-get-records
  [{:keys [project-id] :as client} coll query]
 (let [full-coll (utils/assoc-default-val coll :project-id project-id)
       url (str zyte-storage-root (make-coll-path full-coll))]
   (send-request client
                 {:url url
                  :query-params query
                  :method :get
                  :coerce :json})))

(defn coll-delete-records
  [{:keys [project-id] :as client} coll query]
 (let [full-coll (utils/assoc-default-val coll :project-id project-id)
       url (str zyte-storage-root (make-coll-path full-coll))]
   (send-request client
                 {:url url
                  :query-params query
                  :method :delete
                  :coerce :json})
   true))

(defn make-scrappy-cloud-client
  [{:keys [project-id api-key]}]
  {:api-key api-key :project-id project-id :http-client (atom (ohttp/make-client {}))})

(defn hcf-truncate-frontier
  [impl coordinates]
  (let [slots (hcf-list-slots impl coordinates)
        deleted (doall (for [slot slots] (hcf-delete-slot impl (assoc coordinates :slot slot))))]
    {:slots-deleted (count deleted)}))
