(ns qot.clj-zyte-api-test
  (:require [clojure.test :refer [deftest]]
            [testit.core :refer [facts fact => =in=> =not=>  any]]
            [qot.clj-zyte-api :as api]
            ;; [com.fulcrologic.guardrails.core]
            ))

(deftest frontier-api
  (let [client (api/make-scrappy-cloud-client {:project-id "498050"
                                                :api-key (System/getenv "ZYTE_API_KEY")})]
    (facts "client creation"
           client => map?)

    (let [req1 {:fingerprint "https://example.com/1" :priority 1}
          req2 {:fingerprint "https://example.com/2" :priority 4 :queue-data {:qd "good"}}
          req3 {:fingerprint "https://example.com/3" :priority 3 :fingerprint-data {:fd "bad"}}
          req4 {:fingerprint "https://example.com/4" :priority 2
                :fingerprint-data {:fd "toobad"}
                :queue-data {:qd "toogood"}}
          frontier "frontier1"
          slot (str "api-test-slot-" (System/currentTimeMillis))
          coords {:frontier frontier :slot slot}]
      (facts "requests test for client"
             (api/hcf-add-requests client coords [req1]) => {:requests-added 1}
             (api/hcf-add-requests client coords [req2 req4 req3]) => {:requests-added 3})
      (facts "updates to pending reqs do not work"
             (api/hcf-add-requests client coords [(assoc-in req1 [:queue-data :added] true)])
             => {:requests-added 0}
             (api/hcf-get-batch-requests client coords {:limit 1})
             =not=> {:fingerprint "https://example.com/1" :queue-data {:added true}})
      (facts "test getting fingerprints"
             (api/hcf-list-fingerprints client coords) => [{:fingerprint "https://example.com/1"}
                                                           {:fingerprint "https://example.com/2"}
                                                           {:fingerprint "https://example.com/3"
                                                            :fingerprint-data {:fd "bad"}}
                                                           {:fingerprint "https://example.com/4"
                                                            :fingerprint-data {:fd "toobad"}}])
      (let [batch (api/hcf-get-batch-requests client coords {:limit 3})]
        (facts "get requests tests"
               batch =in=>
               [{:requests [{:fingerprint "https://example.com/1"}]}
                {:requests [{:fingerprint "https://example.com/4" :queue-data {:qd "toogood"}}]}
                {:requests [{:fingerprint "https://example.com/3"}]}])
        (facts "ack requests tests"
               (api/hcf-delete-batch-requests client coords (map :batch-id batch)) => true
               (count (api/hcf-get-batch-requests client coords {:limit 4})) => 1
               (count (api/hcf-list-fingerprints client coords))  => 4))
      (facts "cannot add reqs with existing fingerprints even after delete of req"
             (api/hcf-add-requests client coords [req1]) => {:requests-added 0}
             (count (api/hcf-list-fingerprints client coords))  => 4
             (count (api/hcf-get-batch-requests client coords {:limit 4})) => 1)
      (fact "test getting slots"
            (into #{} (api/hcf-list-slots client (select-keys coords [:frontier]))) => #(contains? % slot))
      (fact "test getting frontiers"
            (into #{} (api/hcf-list-frontiers client {})) => #(contains? % frontier))
      (facts "test deleting slot"
             (api/hcf-delete-slot client coords) => true
             (into #{} (api/hcf-list-slots client (select-keys coords [:frontier]))) =not=> #(contains? % slot))))

  (deftest collection-api
    (let [client (api/make-scrappy-cloud-client {:project-id "498050"
                                                  :api-key (System/getenv "ZYTE_API_KEY")})
          coll-name "colltest"
          coll-type :s
          rec1 {:_key "rec1" :foo "bar"}
          rec2 {:_key "rec2" :foo "bin"}
          rec3 {:_key "rec3" :foo "bat"}
          coll {:collection coll-name :type coll-type}]
      (facts "create and update records"
             (api/coll-upsert-records client coll [rec1 rec2 rec3]) => true
             (api/coll-upsert-records client coll [(assoc rec1 :updated true)]) => true)
      (facts "getting and deleting a record by key"
             (api/coll-get-record client coll "rec1") => {:foo "bar" :updated true}
             (api/coll-delete-record client coll "rec1") => true)
      (facts "getting records by query"
             (api/coll-get-records client coll {:prefix "rec" :meta ["_key" "_ts"] }) =in=> [{:foo "bin"} {:foo "bat"}]
             (api/coll-get-records client coll {:prefix "rec" :prefixcount 1}) => [{:foo "bin"}]
             (api/coll-get-records client coll {:key ["rec2" "rec3"]}) => [{:foo "bin"} {:foo "bat"}]
             (api/coll-get-records client coll {}) => [{:foo "bin"} {:foo "bat"}])
      (facts "delete-rcords-by-query"
             (api/coll-delete-records client coll {:prefix "rec"}) => true
             (api/coll-get-records client coll {}) => []))))
