class ImportResult:
    def __init__(self, status: str, data: list):
        self.status = status
        self.data = data

    def to_dict(self):
        return {"status": self.status, "data": self.data}


class ImportResultsList:
    def __init__(self, status: str, results: list[ImportResult]):
        self.status = status
        self.results = results

    def to_dict(self):
        return {
            "status": self.status,
            "data": [result.to_dict() for result in self.results],
        }
