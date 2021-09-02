(ns qot.clj-zyte-api.impl.scrappy-cloud
  (:require [org.httpkit.client :as ohttp]
            [qot.clj-zyte-api :as api]
            [qot.clj-zyte-api.utils :as utils]
            ;; [clojure.spec.alpha :as s]
            [clojure.string :as str]
            [cheshire.core :as json]))

(def zyte-storage-root "https://storage.scrapinghub.com")

(defn parse-json-lines
  [s]
  (->> (str/split s #"\n")
       (map #(json/decode % true))))

(defn send-request
  [{:keys [api-key client]} request]
  (tap> request)
  (let [full-request (merge {:headers {"accept" "application/json"}
                             :basic-auth (str api-key ":")
                             :client client
                             :as :text}
                            request)
        result @(ohttp/request full-request)]
    (tap> result)
    (cond
      (= :json (:coerce request)) (json/decode (:body result) true)
      (= :json-lines (:coerce request)) (parse-json-lines (:body result))
      :else (:body result))))

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

(defrecord ScrappyCloudClient [api-key project-id client]
  #?@(:bb  []
      :clj [java.lang.AutoCloseable
            (close [_] (.stop client))])
  api/ZyteHcf
  (hcf-add-requests
    [this coords requests]
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
          {:keys [newcount]} (send-request this req)]
      {:requests-added newcount}))
  (hcf-delete-slot
    [this coordinates]
    (let [full-coords (utils/assoc-default-val coordinates :project-id project-id)
          url (str zyte-storage-root (make-hcf-path full-coords))]
      (send-request this
                    {:url url
                     :method :delete
                     :coerce :json-lines})
      true))
  (hcf-get-batch-requests
    [this coordinates {:keys [limit]}]
    (let [full-coords (utils/assoc-default-val coordinates :project-id project-id)
          url (str zyte-storage-root (make-hcf-path full-coords) "/q")
          results (send-request this
                                {:url url
                                 :method :get
                                 :query-params (when limit {:mincount limit})
                                 :coerce :json-lines})]
      (for [{:keys [id requests]} results]
        {:batch-id id :requests (map (fn [[fp data]] (-> {:fingerprint fp}
                                                         (cond-> data (assoc :queue-data data))))
                                     requests)})))
  (hcf-list-fingerprints
    [this coordinates]
    (let [full-coords (utils/assoc-default-val coordinates :project-id project-id)
          url (str zyte-storage-root (make-hcf-path full-coords) "/f")
          fps (send-request this
                            {:url url
                             :method :get
                             :coerce :json-lines})]
      (for [{:keys [fp fdata]} fps]
        (-> {:fingerprint fp}
            (cond-> fdata (assoc :fingerprint-data fdata))))))
  (hcf-delete-batch-requests
    [this coordinates ids]
    (let [full-coords (utils/assoc-default-val coordinates :project-id project-id)
          url (str zyte-storage-root (make-hcf-path full-coords) "/q/deleted")
          body (->> ids
                    (map name)
                    (map json/encode)
                    (str/join "\n"))]
      (send-request this
                    {:url url
                     :method :post
                     :body body
                     :coerce :json-lines})
      true))
  (hcf-list-slots
    [this coordinates]
    (let [full-coords (utils/assoc-default-val coordinates :project-id project-id)
          url (str zyte-storage-root (make-hcf-path full-coords) "/list")
          res (send-request this
                            {:url url
                             :coerce :json
                             :method :get})]
      res))
  (hcf-list-frontiers
    [this coordinates]
    (let [full-coords (utils/assoc-default-val coordinates :project-id project-id)
          url (str zyte-storage-root (make-hcf-path full-coords) "/list")
          res (send-request this
                            {:url url
                             :coerce :json
                             :method :get})]
      res))
  api/ZyteCollection
  (coll-upsert-records
    [this coll data]
    (let [full-coll (utils/assoc-default-val coll :project-id project-id)
          url (str zyte-storage-root (make-coll-path full-coll))
          body (->> data
                    (map json/encode)
                    (str/join "\n"))]
      (send-request this
                    {:url url
                     :body body
                     :method :post})
      true))
  (coll-get-record
    [this coll k]
    (let [full-coll (utils/assoc-default-val coll :project-id project-id)
          url (str zyte-storage-root (make-coll-path full-coll) "/" k)]
      (first (send-request this
                           {:url url
                            :method :get
                            :coerce :json}))))
  (coll-delete-record
    [this coll k]
    (let [full-coll (utils/assoc-default-val coll :project-id project-id)
          url (str zyte-storage-root (make-coll-path full-coll) "/" k)]
      (send-request this
                    {:url url
                     :method :delete
                     :coerce :json})
      true))
  (coll-get-records
    [this coll query]
    (let [full-coll (utils/assoc-default-val coll :project-id project-id)
          url (str zyte-storage-root (make-coll-path full-coll))]
      (send-request this
                    {:url url
                     :query-params query
                     :method :get
                     :coerce :json})))
  (coll-delete-records
    [this coll query]
    (let [full-coll (utils/assoc-default-val coll :project-id project-id)
          url (str zyte-storage-root (make-coll-path full-coll))]
      (send-request this
                    {:url url
                     :query-params query
                     :method :delete
                     :coerce :json})
      true)))

(defn make-scrappy-cloud-client
  [{:keys [project-id api-key]}]
  (ScrappyCloudClient. api-key project-id (ohttp/make-client {})))
