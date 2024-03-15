import java.util.HashMap

def transform(event):
    event['done'] = True

def configure(properties):
    pass

settings = java.util.HashMap()
settings.put("configure", "configure")
settings.put("transform", "transform")
