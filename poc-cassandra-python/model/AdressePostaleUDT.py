from cassandra.cqlengine.columns import *
from cassandra.cqlengine.usertype import UserType

class AdressePostaleUDT(UserType):
    no_civique = Integer(required=True)
    rue = Text(required=True)
    ville = Text(required=True)
    code_postal = Text(required=True)
