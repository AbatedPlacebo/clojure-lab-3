(ns clojure-lab-3.parallel)

(defn my-filter-reduce [func lst]
  (reverse (reduce (fn [new-lst elem]
                     (if (func elem)
                       (conj new-lst elem)
                       new-lst)) '() lst)))


(defn my-filter [predicate coll] (filter predicate coll))
(def split-into-chunks
  (fn [n coll]
    (loop [res nil
           it_lst coll]
      (if (empty? it_lst)
        (reverse res)
        (recur (cons (take n it_lst) res) (drop n it_lst))))))

(defn filter-in-parallel [predicate coll chunk-size]
  (let [chunks (split-into-chunks chunk-size coll)]
    (mapv (fn [chunk]
              (future (my-filter predicate chunk)))  chunks)))
(defn await-results [futures]  (mapcat deref futures))
(def data (range 100000))
(def n-workers (Integer/valueOf 10))
(def chunk-size (/ (count data) n-workers))
(do
(time (do
        (def futures (filter-in-parallel odd? data chunk-size))
        (def filtered-data (await-results futures))))
(time (my-filter-reduce odd? (range 100000)))
)
