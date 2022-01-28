(ns user
  (:require [qot.clj-zyte-api :as api]))

(def scrappy-cloud-test-project-id "498050")

(defn make-cc-client
  []
  (api/make-scrappy-cloud-client {:project-id scrappy-cloud-test-project-id
                                   :api-key (System/getenv "ZYTE_API_KEY")}))
