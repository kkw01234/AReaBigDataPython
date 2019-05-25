
class Restaurant:

    def __init__(self,oid,name,date,r_addr,r_lat,r_lng,r_name,comment):
        self.oid = oid
        self.name = name
        self.date = date
        self.r_addr = r_addr
        self.r_lat = r_lat
        self.r_lng = r_lng
        self.r_name = r_name
        self.comment = comment

    def get_comment(self):
        return self.comment
