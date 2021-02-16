(ns qot.clj-zyte-api.impl.memory
  (:require [qot.clj-zyte-api :as api]
            [qot.clj-zyte-api.utils :as utils]
            [clojure.spec.alpha :as s]
            [clojure.edn :as edn]))

(defn make-hcf-path
  [{:keys [project-id frontier slot] :as coords}]
  (->> (cond (and project-id frontier slot) [:hcf project-id frontier slot]
             (and project-id frontier) [:hcf project-id frontier]
             project-id [:hcf project-id]
             :else (throw (ex-info "Insufficient Frontier API coordinates given" {:coordinates coords})))
       (mapv name)))

(defrecord MemoryClient [project-id db-req db-fp]
  api/ZyteHcf
  (hcf-add-requests
    [_ coords requests]
    (let [full-coords (utils/assoc-default-val coords :project-id project-id)
          path (make-hcf-path full-coords)]
      (dosync
       (let [fp-state @db-fp
             {:keys [fps reqs]} (reduce
                                 (fn [acc {:keys [fingerprint] :as req}]
                                   (if (get-in fp-state (conj path fingerprint))
                                     acc
                                     (-> acc
                                         (assoc-in [:fps fingerprint]
                                                   (select-keys req [:fingerprint :fingerprint-data]))
                                         (assoc-in [:reqs fingerprint]
                                                   (select-keys req [:fingerprint :queue-data :priority])))))
                                 {} requests)]
         (doseq [[db add] [[db-fp fps] [db-req reqs]]]
           (alter db (fn [db add] (update-in db path (fn [old add] (merge old add)) add)) add))
         {:requests-added (count fps)}))))
  (hcf-delete-slot
    [_ coordinates]
    (let [full-coords (utils/assoc-default-val coordinates :project-id project-id)
          path (make-hcf-path full-coords)]
      (dosync
       (doseq [db [db-fp db-req]]
         (alter db
                (fn [db-val path k]
                  (update-in db-val path #(dissoc %1 %2) k))
                (butlast path)
                (last path))))
      true))
  (hcf-get-batch-requests
    [_ coordinates {:keys [limit]}]
    (let [full-coords (utils/assoc-default-val coordinates :project-id project-id)
          path (make-hcf-path full-coords)
          slot (get-in @db-req path)
          reqs (take limit (sort-by :priority (vals slot)))]
      (map (fn [{:keys [fingerprint] :as req}]
             {:batch-id (pr-str {:bid [fingerprint]})
              :requests (list (select-keys req [:fingerprint :queue-data]))})
           reqs)))
  (hcf-list-fingerprints
    [_ coordinates]
    (let [full-coords (utils/assoc-default-val coordinates :project-id project-id)
          path (make-hcf-path full-coords)
          fps (->> (get-in @db-fp path)
                   (vals)
                   (map #(select-keys % [:fingerprint :fingerprint-data]))
                   (sort-by :fingerprint))]
      fps))
  (hcf-delete-batch-requests
    [_ coordinates enc-ids]
    (let [full-coords (utils/assoc-default-val coordinates :project-id project-id)
          path (make-hcf-path full-coords)
          ids (mapcat (comp :bid edn/read-string) enc-ids)]
      (dosync
       (alter db-req (fn [db-val ids] (update-in db-val path #(apply dissoc %1 %2) ids))
              ids))
      true))
  (hcf-list-slots
    [_ coordinates]
    (let [full-coords (utils/assoc-default-val coordinates :project-id project-id)
          path (make-hcf-path full-coords)]
      (keys (get-in @db-req path))))
  (hcf-list-frontiers
    [_ coordinates]
    (let [full-coords (utils/assoc-default-val coordinates :project-id project-id)
          path (make-hcf-path full-coords)]
      (keys (get-in @db-req path)))))

(defn make-memory-client
  [{:keys [project-id]}]
  (MemoryClient. project-id (ref {}) (ref {})))
