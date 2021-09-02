(ns qot.clj-zyte-api
  (:require [com.fulcrologic.guardrails.core :refer [>defn >def >fdef | ? =>]]
            [clojure.spec.alpha :as s]))

(declare ZyteHcf)

(>def :zyte/project-id (s/and string? #(re-matches #"\d+" %)))
(>def :zyte/project-coordinates (s/keys :opt-un [:zyte/project-id]))
(>def :zyte/client #(extends? ZyteHcf %))

(>def :zyte-frontier/frontier string?)
(>def :zyte-frontier/slot string?)
(>def :zyte-frontier/frontier-coordinates
      (s/merge
       :zyte/project-coordinates
       (s/keys :req-un [:zyte-frontier/frontier])))
(>def :zyte-frontier/slot-coordinates
      (s/merge :zyte-frontier/frontier-coordinates
               (s/keys :req-un [:zyte-frontier/slot])))

(>def :zyte-frontier/requests-added pos-int?)
(>def :zyte-frontier/requests-return (s/keys :req-un [:zyte-frontier/requests-added]))

(>def :zyte-frontier/fingerprint string?)
(>def :zyte-frontier/queue-data map?)
(>def :zyte-frontier/fingerprint-data map?)
(>def :zyte-frontier/priority nat-int?)


(>def :zyte-frontier/request (s/keys :req-un [:zyte-frontier/fingerprint]
                                     :opt-un [:zyte-frontier/queue-data
                                              :zyte-frontier/fingerprint-data
                                              :zyte-frontier/priority]))

(>fdef hcf-add-requests
       [impl coordinates requests]
       [:zyte/client :zyte-frontier/slot-coordinates (s/coll-of :zyte-frontier/requestx)
        => :zyte-frontier/requests-return])

(>fdef hcf-delete-slot
       [impl coordinates]
       [:zyte/client :zyte-frontier/slot-coordinates => int?])

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
  (coll-get-records [impl coll query])
  (coll-delete-record [impl coll k])
  (coll-delete-records [impl coll query])
  (coll-list-records [impl coll])
  (coll-list-collections [impl])
  (coll-count-records [impl coll])
  (coll-delete-sparse-records [impl coll ks])
  (coll-delete-collection [impl coll])
  (coll-rename-collection [impl coll new-name]))
