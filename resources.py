from flask_restful import Resource
import medium_blogger_discovery

class Twitter(Resource):
    def get(self):
        medium_blogger_discovery.run_the_process()
        return "Process complete"