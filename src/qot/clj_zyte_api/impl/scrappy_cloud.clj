(ns qot.clj-zyte-api.impl.scrappy-cloud
  (:require [clj-http.client :as http]
            [qot.clj-zyte-api :as api]
            [qot.clj-zyte-api.utils :as utils]
            [clojure.spec.alpha :as s]
            [clojure.string :as str]
            [cheshire.core :as json]))

(def zyte-storage-root "https://storage.scrapinghub.com")

(s/def :zyte/project-id (s/and string? #(re-matches #"\d+" %)))
(s/def :zyte-frontier/frontier string?)
(s/def :zyte-frontier/slot string?)
(s/def :zyte-frontier/slot-coordinates (s/keys :req-un [:zyte-frontier/frontier
                                                        :zyte-frontier/slot]
                                               :opt-un [:zyte/project-id]))
(s/def :zyte-frontier/frontier-coordinates (s/keys :req-un [:zyte-frontier/frontier]
                                                   :opt-un [:zyte/project-id]))
(s/def :zyte/project-coordinates (s/keys :opt-un [:zyte/project-id]))

(defn parse-json-lines
  [s]
  (->> (str/split s #"\n")
       (map #(json/decode % true))))

(defn send-request
  [{:keys [api-key]} request]
  (tap> request)
  (let [full-request (merge {:headers {:Accept "application/json"}
                             :basic-auth (str api-key ":")}
                            request)
        result (http/request full-request)]
    (tap> result)
    (if (:as request)
      (:body result)
      (parse-json-lines (:body result)))))

(s/fdef make-hcf-path
  :args (s/cat :coordinates (s/or :slot-coordinates :zyte-frontier/slot-coordinates
                                  :frontier-coordinates :zyte-frontier/frontier-coordinates
                                  :project-coordinates :zyte/project-coordinates))
  :ret string?)

(defn make-hcf-path
  [{:keys [project-id frontier slot] :as coords}]
  (cond (and project-id frontier slot) (format "/hcf/%s/%s/s/%s" project-id frontier slot)
        (and project-id frontier) (format "/hcf/%s/%s" project-id frontier)
        project-id (format "/hcf/%s" project-id)
        :else (throw (ex-info "Insufficient Frontier API coordinates given" {:coordinates coords}))))

(defrecord ScrappyCloudClient [api-key project-id]
  api/ZyteHcf
  (hcf-add-requests
    [_ coords requests]
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
               :as :json
               :body (str/join "\n" lines)
               :content-type :json}
          {:keys [newcount]} (send-request {:api-key api-key} req)]
      {:requests-added newcount}))
  (hcf-delete-slot
    [_ coordinates]
    (let [full-coords (utils/assoc-default-val coordinates :project-id project-id)
          url (str zyte-storage-root (make-hcf-path full-coords))]
      (send-request {:api-key api-key} {:url url :method :delete})
      true))
  (hcf-get-batch-requests
    [_ coordinates {:keys [limit]}]
    (let [full-coords (utils/assoc-default-val coordinates :project-id project-id)
          url (str zyte-storage-root (make-hcf-path full-coords) "/q")
          results (send-request {:api-key api-key}
                                {:url url
                                 :method :get
                                 :query-params (when limit {:mincount limit})})]
      (for [{:keys [id requests]} results]
        {:batch-id id :requests (map (fn [[fp data]] (-> {:fingerprint fp}
                                                         (cond-> data (assoc :queue-data data))))
                                     requests)})))
  (hcf-get-fingerprints
    [_ coordinates]
    (let [full-coords (utils/assoc-default-val coordinates :project-id project-id)
          url (str zyte-storage-root (make-hcf-path full-coords) "/f")
          fps (send-request {:api-key api-key}
                            {:url url
                             :method :get})]
      (for [{:keys [fp fdata]} fps]
        (-> {:fingerprint fp}
            (cond-> fdata (assoc :fingerprint-data fdata)))))))

(defn make-scrappy-cloud-client
  [{:keys [project-id api-key]}]
  (ScrappyCloudClient. api-key project-id))
