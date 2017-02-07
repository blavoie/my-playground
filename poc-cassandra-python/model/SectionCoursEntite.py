from cassandra.cqlengine import columns
from cassandra.cqlengine.models import Model

class SectionCours(Model):
    __table_name__ = 'section_cours'

    code_session = columns.Text(primary_key=True, partition_key=True)
    nrc = columns.Text(primary_key=True, partition_key=True)
    titre = columns.Text(required=True)
    sigle_matiere = columns.Text(required=True)
    numero_cours = columns.Text(required=True)

    date_debut = columns.Date()
    date_fin = columns.Date()

    inscriptions = columns.Set(value_type=columns.BigInt())
