(ns qot.clj-zyte-api
  (:require [clojure.spec.alpha :as s]))

(s/def :zyte-frontier/requests-added pos-int?)

(s/fdef hcf-add-request
  :args (s/cat :impl record?
               :slot-coords :zyte-fontier/slot-coordinates
               :fingerprint string?
               :frontier-options (s/? (s/keys :opt-un [:zyte-frontier/queue-data
                                                       :zyte-frontier/fingerprint-data
                                                       :zyte-frontier/priority])))
  :ret (s/keys :req-un [:zyte-frontier/requests-added]))

(defprotocol ZyteHcf
  (hcf-add-requests [impl coordinates requests])
  (hcf-delete-slot [impl coordinates])
  (hcf-get-batch-requests [impl coordinates options])
  (hcf-delete-batch-requests [impl coordinates ids])
  (hcf-list-fingerprints [impl coordinates])
  (hcf-list-slots [impl coordinates])
  (hcf-list-frontiers [impl coordinates]))

(defn hcf-truncate-frontier
  [impl coordinates]
  (let [slots (hcf-list-slots impl coordinates)
        deleted (doall (for [slot slots] (hcf-delete-slot impl (assoc coordinates :slot slot))))]
    {:slots-deleted (count deleted)}))

(defprotocol ZyteCollection
  (coll-upsert-records [impl coll data])
  (coll-get-record [impl coll k])
  (coll-delete-record [impl coll k])
  (coll-list-records [impl coll])
  (coll-list-collections [impl]))
