# -*- coding: utf-8 -*-

import cx_Oracle
from oracle.utils import named_tuple

from cassandra.cluster import Cluster
from cassandra.cqlengine import connection
from cassandra.policies import RetryPolicy, TokenAwarePolicy, DCAwareRoundRobinPolicy

from model.cassandra_types import SectionCours
from model.cassandra_types import Individu

ora_user = 'brlav35'
ora_pass = ''
ora_db = 'ul-ena-pr-bd10.ul.ca:1521/enpr12.ul.ca'
ora_arraysize = 500;
ora_nb = 10000

ora_conn = cx_Oracle.connect(ora_user, ora_pass, ora_db)

### Cassandra Connection

cas_keyspace = 'brlav35'

### Cassandra Connection

cluster = Cluster(contact_points=["n01.cl01", "n02.cl01"],
                  protocol_version=3,
                  default_retry_policy=RetryPolicy(),
                  load_balancing_policy=TokenAwarePolicy(DCAwareRoundRobinPolicy(local_dc='datacenter1')))
session = cluster.connect(keyspace=cas_keyspace)
#session.row_factory = dict_factory

#connection.set_session(session)
connection.setup(hosts=["n01.cl01", "n02.cl01"], default_keyspace=cas_keyspace, protocol_version=3)

### Récupérer et importer utilisateurs

def importer_individu():
    ora_cur = ora_conn.cursor()
    ora_cur.arraysize = ora_arraysize
    ora_cur.execute("""
    SELECT di.numero_dossier, di.nom, di.prenom, di.code_utilisateur, di.adresse_courriel_principale
    FROM icu.dossier_individu di
    WHERE    exists
                 (SELECT *
                  FROM icu.inscription_cours_mc inco
                  WHERE inco.numero_dossier_pidm = di.numero_dossier)
          OR exists
                 (SELECT *
                  FROM icu.enseignant_section_mc ense
                  WHERE ense.numero_dossier_pidm = di.numero_dossier)
    """
                    )

    nb = 0
    rows = ora_cur.fetchmany()
    while rows:
        for row in rows:
            row = named_tuple(row, ora_cur)
            print row

            # Better way to do this?
            # Récupérer les colonnes communes entre le modèle et ce qui est retourné par la requête.
            # Permet de faciliter les multiples entités de destination pour une même source.
            # Déplacer / Mettre en cache la liste des colonnes communes.

            cols = set(Individu._columns.keys()).intersection(row.keys())
            row = {k: row[k] for k in cols}

            Individu.create(**row)

        # next bulk
        if nb >= ora_nb:
            break

        rows = ora_cur.fetchmany()


### Récupérer et importer sections de cours
def importer_section_cours():
    ora_cur = ora_conn.cursor()
    ora_cur.arraysize = ora_arraysize
    ora_cur.execute("""
    SELECT seco.code_session
          ,seco.numero_reference_section_cours AS nrc
          ,seco.titre_section_cours AS titre
          ,seco.sigle_matiere
          ,seco.numero_cours
          ,coalesce(seco.date_debut_section, sess.date_debut_cours) AS date_debut
          ,coalesce(seco.date_fin_section, sess.date_fin_cours) AS date_fin
          , (SELECT cast (collect (inco.numero_dossier_pidm) AS icu.t_number10_tab)
             FROM icu.inscription_cours_mc inco
             WHERE     inco.code_session = seco.code_session
                   AND inco.numero_reference_section_cours = seco.numero_reference_section_cours)
               AS inscriptions
          , (SELECT cast (collect (ense.numero_dossier_pidm) AS icu.t_number10_tab)
             FROM icu.enseignant_section_mc ense
             WHERE     ense.code_session = seco.code_session
                   AND ense.numero_reference_section_cours = seco.numero_reference_section_cours)
               AS enseignants
    FROM icu.section_cours_mc seco
             INNER JOIN icu.session_mc sess
             ON seco.code_session = sess.codinscriptions_par_section_course_session
    WHERE seco.code_statut_section_cours = 'A'
    """)

    nb = 0
    rows = ora_cur.fetchmany()
    while rows:
        for row in rows:
            row = named_tuple(row, ora_cur)
            print row
            sc = SectionCours.create(**row)

            # next bulk
            if nb >= ora_nb:
                break

        rows = ora_cur.fetchmany()


########################
## Exécuter

# importer_section_cours()
importer_individu()


########################
## cleanup

cluster.shutdown()
