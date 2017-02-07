# -*- coding: utf-8 -*-

import cx_Oracle

from cassandra.cluster import Cluster
from cassandra.policies import RetryPolicy, TokenAwarePolicy, DCAwareRoundRobinPolicy

from cassandra.cqlengine import connection
from cassandra.query import dict_factory

from cassandra.cqlengine import columns
from cassandra.cqlengine.models import Model

### Oracle Connection
from model import SectionCoursEntite
from model.SectionCoursEntite import SectionCours

ora_user = 'brlav35'
ora_pass = ''
ora_db = 'ul-ena-pr-bd10.ul.ca:1521/enpr12.ul.ca'
ora_arraysize = 500;
ora_nb = 10000

ora_conn = cx_Oracle.connect(ora_user, ora_pass, ora_db)

def columns_names(cur):
    names = []
    for desc in cur.description:
        name = desc[0]
        names.append(name)

    return names

def named_tuple(tuple, cur, to_lower=True):
    names = columns_names(cur)

    if to_lower:
        names = map(lambda x: x.lower(), names)

    return dict(zip(names, tuple))


### Cassandra Connection

cas_keyspace = 'brlav35'

### Cassandra Connection

cluster = Cluster(contact_points=["n01.cl01", "n02.cl01"],
                  default_retry_policy=RetryPolicy(),
                  load_balancing_policy=TokenAwarePolicy(DCAwareRoundRobinPolicy(local_dc='datacenter1')))
session = cluster.connect(keyspace='brlav35')
session.row_factory = dict_factory

connection.set_session(session)

### Define UDT, and register
"""
class AdressePostale(object):
    def __init__(self, no_civique, rue, ville, code_postal):
        self.no_civique = no_civique
        self.rue = rue
        self.ville = ville
        self.code_postal = code_postal

cluster.register_user_type(cas_keyspace, 'adr_postale_udt', AdressePostale)
"""

### Récupérer et importer utilisateurs
"""
ora_cur = ora_conn.cursor()
ora_cur.arraysize = ora_arraysize
ora_cur.execute(
    "select di.numero_dossier, di.code_utilisateur, di.nom, di.prenom from icu.dossier_individu di order by dbms_random.value"
)

nb = 0
rows = ora_cur.fetchmany()
while rows:

    for row in rows:
        print row

    # next bulk
    if nb >= ora_nb:
        break

    rows = ora_cur.fetchmany()
"""


### Récupérer et importer sections de cours

ora_cur = ora_conn.cursor()
ora_cur.arraysize = ora_arraysize
ora_cur.execute("""
select seco.code_session
      ,seco.numero_reference_section_cours as nrc
      ,seco.titre_section_cours as titre
      ,seco.sigle_matiere
      ,seco.numero_cours
      ,seco.date_debut_section as date_debut
      ,seco.date_fin_section as date_fin
      , (select cast (collect (inco.numero_dossier_pidm) as icu.t_number10_tab)
         from icu.inscription_cours_mc inco
         where     inco.code_session = seco.code_session
               and inco.numero_reference_section_cours = seco.numero_reference_section_cours)
           as liste_inscriptions
from icu.section_cours_mc seco
where seco.code_statut_section_cours = 'A'
""")

nb = 0
rows = ora_cur.fetchmany()
while rows:

    for row in rows:
        row = named_tuple(row, ora_cur)
        print row
        sc = SectionCours.create(code_session=row['code_session'], nrc=row['nrc'], sigle_matiere=row['sigle_matiere'],
                                 numero_cours=row['numero_cours'], titre=row['titre'], date_debut=row['date_debut'],
                                 date_fin=row['date_fin'], inscriptions=row['liste_inscriptions'])

        # next bulk
        if nb >= ora_nb:
            break

    rows = ora_cur.fetchmany()

########################
## cleanup

cluster.shutdown()
