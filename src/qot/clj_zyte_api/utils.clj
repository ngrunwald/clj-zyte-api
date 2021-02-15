(ns qot.clj-zyte-api.utils)

(defn assoc-default-val
  [m k v]
  (update m
          k
          (fn [old n] (if (nil? old) n old))
          v))
