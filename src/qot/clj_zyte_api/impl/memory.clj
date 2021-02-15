(ns qot.clj-zyte-api.impl.memory
  (:require [qot.clj-zyte-api :as api]
            [qot.clj-zyte-api.utils :as utils]
            [clojure.spec.alpha :as s]))

(defn make-hcf-path
  [{:keys [project-id frontier slot] :as coords}]
  (->> (cond (and project-id frontier slot) [:hcf project-id frontier slot]
             (and project-id frontier) [:hcf project-id frontier]
             project-id [:hcf project-id]
             :else (throw (ex-info "Insufficient Frontier API coordinates given" {:coordinates coords})))
       (mapv name)))

(defrecord MemoryClient [project-id db]
  api/ZyteHcf
  (hcf-add-requests
    [_ coords requests]
    (let [full-coords (utils/assoc-default-val coords :project-id project-id)
          indexed-reqs (reduce (fn [acc {:keys [fingerprint] :as req}] (assoc acc fingerprint req))
                               {} requests)
          path (make-hcf-path full-coords)]
      (dosync
       (let [current-count (count (get-in @db path))]
         (alter db #(update-in %1 path merge %2) indexed-reqs)
         {:requests-added (- (count (get-in @db path)) current-count)}))))
    (hcf-delete-slot
      [_ coordinates]
      (let [path (make-hcf-path coordinates)]
        (dosync
         (alter db
                (fn [db-val path k]
                  (update-in db-val path #(dissoc %1 %2) k))
                (butlast path)
                (last path)))
        true))
    (hcf-get-batch-requests
      [_ coordinates {:keys [limit]}]
      (let [full-coords (utils/assoc-default-val coordinates :project-id project-id)
            path (make-hcf-path full-coords)
            slot (get-in @db path)
            reqs (take limit (sort-by :priority (vals slot)))]
        (map (fn [{:keys [fingerprint] :as req}]
               {:batch-id (str "bid__" fingerprint)
                :requests (list (select-keys req [:fingerprint :queue-data]))})
             reqs)))
    (hcf-get-fingerprints
      [_ coordinates]
      (let [full-coords (utils/assoc-default-val coordinates :project-id project-id)
            path (make-hcf-path full-coords)
            fps (->> (get-in @db path)
                     (vals)
                     (map #(select-keys % [:fingerprint :fingerprint-data]))
                     (sort-by :fingerprint))]
        fps)))

(defn make-memory-client
  [{:keys [project-id seed]}]
  (MemoryClient. project-id (ref seed)))
