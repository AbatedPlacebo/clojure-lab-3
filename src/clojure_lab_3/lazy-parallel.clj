(ns clojure-lab-3.lazy-parallel)

(defn my-mapcat
  [f coll]
  (lazy-seq
   (if (not-empty coll)
     (concat
      (f (first coll))
      (my-mapcat f (rest coll))))))

(defn my-filter-reduce [func lst]
  (reverse (reduce (fn [new-lst elem]
                     (if (func elem)
                       (conj new-lst elem)
                       new-lst)) '() lst)))


(defn my-filter [predicate coll] (doall (filter predicate coll)))

(def split-into-chunks
  (fn [n coll]
    (loop [res nil
           it_lst coll]
      (if (empty? it_lst)
        (reverse res)
        (recur (cons (take n it_lst) res) (drop n it_lst))))))


(defn split-into-lazy-chunks
  ([n col] 
   (lazy-seq 
     (cons (take n col) (split-into-lazy-chunks n (drop n col)))))
  )

(defn lazy-filter-in-parallel [predicate coll chunk-size]
  (let [chunks (take chunk-size (split-into-lazy-chunks chunk-size coll))]
    (map (fn [chunk]
              (future (my-filter predicate chunk))) chunks)
    )
  )

(defn await-results [futures] (my-mapcat deref futures))

(def data (range 100000))

(def n-workers (Integer/valueOf 1000))

(def chunk-size (/ (count data) n-workers))

(defn my-odd [x] (odd? x))

(do
(def futures (lazy-filter-in-parallel my-odd data chunk-size))
(time (def filtered-data (doall (await-results futures))))
(def futures (lazy-filter-in-parallel my-odd data (- 1 chunk-size)))
(time (def filtered-data (doall (await-results futures))))
(time (doall (filter my-odd data)))
)
