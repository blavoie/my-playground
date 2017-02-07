from cassandra.cqlengine import columns
from cassandra.cqlengine.models import Model
from model import AdressePostaleUDT

class IndividuEntite(Model):
    numero_dossier = columns.BigInt(primary_key=True)
    nom = columns.Text(required=True)
    prenom = columns.Text(required=True)
    avatar = columns.Blob
    prefs_jsp = columns.Text

    adr_postales = columns.Map(columns.Text, AdressePostaleUDT)
