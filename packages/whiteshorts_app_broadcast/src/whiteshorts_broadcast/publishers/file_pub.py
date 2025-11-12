import json

class FilePublisher:
    def __init__(self, cfg):
        self.cfg = cfg

    def publish(self, rows):
        with open(self.cfg.out_json_path, "w") as f:
            json.dump(rows, f, indent=2)
