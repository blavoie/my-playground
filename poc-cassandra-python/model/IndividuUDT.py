from cassandra.cqlengine.columns import *
from cassandra.cqlengine.usertype import UserType

class IndividuUDT(UserType):
    numero_dossier = BigInt(required=True)
    code_utilisateur = Text(required=True)
    nom = Text(required=True)
    prenom = Text(required=True)

