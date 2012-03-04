(ns playground.test-data)

(def person-data
  [
    ;; [person]
    ["alice"]
    ["bob"]
    ["chris"]
    ["david"]
    ["emily"]
    ["george"]
    ["gary"]
    ["harold"]
    ["kumar"]
    ["luanne"]
    ])

(def age-data
  [
    ;; [person age]
    ["alice" 28]
    ["bob" 33]
    ["chris" 40]
    ["david" 25]
    ["emily" 25]
    ["george" 31]
    ["gary" 28]
    ["kumar" 27]
    ["luanne" 36]
    ])

(def gender-data
  [
    ;; [person gender]
    ["alice" "f"]
    ["bob" "m"]
    ["chris" "m"]
    ["david" "m"]
    ["emily" "f"]
    ["george" "m"]
    ["gary" "m"]
    ["harold" "m"]
    ["luanne" "f"]
    ])

(def follows-data
  [
    ;; [person-follower person-followed]
    ["alice" "david"]
    ["alice" "bob"]
    ["alice" "emily"]
    ["bob" "david"]
    ["bob" "george"]
    ["bob" "luanne"]
    ["david" "alice"]
    ["david" "luanne"]
    ["emily" "alice"]
    ["emily" "bob"]
    ["emily" "george"]
    ["emily" "gary"]
    ["george" "gary"]
    ["harold" "bob"]
    ["luanne" "harold"]
    ["luanne" "gary"]
    ])

(def location-data
  [
    ;; [person country state city]
    ["alice" "usa" "california" nil]
    ["bob" "canada" nil nil]
    ["chris" "usa" "pennsylvania" "philadelphia"]
    ["david" "usa" "california" "san francisco"]
    ["emily" "france" nil nil]
    ["gary" "france" nil "paris"]
    ["luanne" "italy" nil nil]
    ])
