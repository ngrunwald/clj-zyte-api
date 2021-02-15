(ns qot.clj-zyte-api-test
  (:require [clojure.test :refer :all]
            [testit.core :refer :all]
            [qot.clj-zyte-api :refer :all]
            [qot.clj-zyte-api.impl.memory :as mem]
            [qot.clj-zyte-api.impl.scrappy-cloud :as sc]))

(deftest api
  (let [mem-client (mem/make-memory-client {:project-id "498050"})
        cloud-client (sc/make-scrappy-cloud-client {:project-id "498050"
                                                    :api-key (System/getenv "ZYTE_API_KEY")})]
    (facts "clients creation"
        mem-client => record?
        cloud-client => record?)

    (let [req1 {:fingerprint "https://example.com/1" :priority 1}
          req2 {:fingerprint "https://example.com/2" :priority 4 :queue-data {:qd "good"}}
          req3 {:fingerprint "https://example.com/3" :priority 3 :fingerprint-data {:fd "bad"}}
          req4 {:fingerprint "https://example.com/4" :priority 2
                :fingerprint-data {:fd "toobad"}
                :queue-data {:qd "toogood"}}
          coords {:frontier "frontier1" :slot (str "api-test-slot-" (System/currentTimeMillis))}]
      (doseq [[client client-name] [[cloud-client "scrappy-cloud-client"]
                                    [mem-client "memory-client"]
                                    ]]
        (facts "requests test for client"
               (hcf-add-requests client coords [req1]) => {:requests-added 1}
               (hcf-add-requests client coords [req2 req4 req3]) => {:requests-added 3})
        (facts "updates to pending reqs do not work"
               (hcf-add-requests client coords [(assoc-in req1 [:queue-data :added] true)]) => {:requests-added 0}
               (hcf-get-batch-requests client coords {:limit 1})
               =not=> {:fingerprint "https://example.com/1" :queue-data {:added true}})
        (facts "test getting fingerprints"
               (hcf-get-fingerprints client coords) => [{:fingerprint "https://example.com/1"}
                                                        {:fingerprint "https://example.com/2"}
                                                        {:fingerprint "https://example.com/3"
                                                         :fingerprint-data {:fd "bad"}}
                                                        {:fingerprint "https://example.com/4"
                                                         :fingerprint-data {:fd "toobad"}}])
        (let [batch (hcf-get-batch-requests client coords {:limit 3})]
          (facts "get requests tests"
                  batch =in=>
                 [{:requests [{:fingerprint "https://example.com/1"}]}
                  {:requests [{:fingerprint "https://example.com/4" :queue-data {:qd "toogood"}}]}
                  {:requests [{:fingerprint "https://example.com/3"}]}])
          (facts "ack requests tests"
                 (hcf-delete-batch-requests client coords (map :batch-id batch)) => true
                 (count (hcf-get-batch-requests client coords {:limit 4})) => (partial = 1)
                 (count (hcf-get-fingerprints client coords))  => (partial = 4)
                 (hcf-add-requests client coords [req1]) => {:requests-added 0}
                 (count (hcf-get-fingerprints client coords))  => (partial = 4)
                 (count (hcf-get-batch-requests client coords {:limit 4})) => (partial = 1))
          )
        ))))
