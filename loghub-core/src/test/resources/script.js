var configure = function(properties) {
};

var transform = function(event) {
  event.put("done", true);
};

var a = new java.util.HashMap();
a.put("configure", "configure")
a.put("transform", "transform")
a;
