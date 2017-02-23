# -*- coding: utf-8 -*-

from __future__ import absolute_import

from cassandra.cqlengine import columns
from cassandra.cqlengine.management import sync_table
from cassandra.cqlengine.models import Model
from cassandra.cqlengine.usertype import UserType


class AdressePostaleUDT(UserType):
    __type_name__ = 'adr_postale_udt'

    no_civique = columns.Integer(required=True)
    rue = columns.Text(required=True)
    ville = columns.Text(required=True)
    code_postal = columns.Text(required=True)


class IndividuUDT(UserType):
    __type_name__ = 'inidividu_udt'

    numero_dossier = columns.BigInt(required=True)
    code_utilisateur = columns.Text(required=True)
    nom = columns.Text(required=True)
    prenom = columns.Text(required=True)


# Entités

class Individu(Model):
    __table_name__ = 'individu'

    numero_dossier = columns.BigInt(primary_key=True)
    code_utilisateur = columns.Text(required=False)
    nom = columns.Text(required=True)
    prenom = columns.Text(required=True)
    avatar = columns.Blob
    prefs_jsp = columns.Text

    # Nécessite driver 3.6.0
    # https://datastax-oss.atlassian.net/browse/PYTHON-649
    # adr_postales = columns.Map(columns.Text, columns.UserDefinedType(AdressePostaleUDT))
    adr_courriels = columns.Map(columns.Text, columns.Text)


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
    enseignants = columns.Set(value_type=columns.BigInt())


class InscriptionParIndividu(Model):
    __table_name__ = 'inscriptions_par_individu'

    numero_dossier = columns.BigInt(primary_key=True, partition_key=True)
    code_session = columns.Text(primary_key=True, clustering_order='DESC')
    nrc = columns.Text(primary_key=True, clustering_order='ASC')

    titre = columns.Text(required=True)

    note_finale = columns.Text()


class InscriptionParSectionCours(Model):
    __table_name__ = 'inscriptions_par_section_cours'

    code_session = columns.Text(primary_key=True, partition_key=True)
    nrc = columns.Text(primary_key=True, partition_key=True)
    numero_dossier = columns.BigInt(primary_key=True)

    nom = columns.Text(required=True)
    prenom = columns.Text(required=True)

    note_finale = columns.Text()


class InscriptionParSectionCours2(Model):
    __table_name__ = 'inscriptions_par_section_cours_2'

    code_session = columns.Text(primary_key=True, partition_key=True)
    nrc = columns.Text(primary_key=True, partition_key=True)
    nom = columns.Text(primary_key=True, clustering_order="ASC")
    prenom = columns.Text(primary_key=True, clustering_order="ASC")
    numero_dossier = columns.BigInt(primary_key=True)

    note_finale = columns.Text()
