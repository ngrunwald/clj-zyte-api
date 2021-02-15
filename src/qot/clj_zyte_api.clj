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
  (hcf-get-fingerprints [impl coordinates])
  (hcf-get-slots [impl coordinates])
  (hcf-get-frontiers [impl coordinates]))
