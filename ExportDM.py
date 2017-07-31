from py2neo import Graph, Node, Relationship
import xlrd, datetime

def create_neo4jgraph(graph, dm_file):
    sheet = dm_file.sheet_by_index(0)
    graph.schema.create_index("Study", "study_id")
    graph.schema.create_index("Country", "country_name")
    graph.schema.create_index("Site", "site_id")
    graph.schema.create_index("Investigator", "investigator_name")
    graph.schema.create_index("Subject", "u_subject_id")
    graph.schema.create_index("Compound", "dm_compound")
    graph.schema.create_index("Therapeutic","dm_therapeutic_area")
    graph.schema.create_index("Demographic","dm_id")


    for r in range(1, 200):
        # Extract the required values from the excel sheets and store them.
        # At present the functionality extracts all values from a single excel sheet

        dm_id = str(sheet.cell(r, 0).value)
        study_id = str(sheet.cell(r, 1).value)
        subject_id = str(sheet.cell(r,3).value)

        age_calculated_date = None if (sheet.cell(r, 5).value == '') else str(datetime.datetime(
            *xlrd.xldate_as_tuple(sheet.cell(r, 5).value, dm_file.datemode)))
        first_treatment_date = None if (sheet.cell(r, 7).value == '') else str(datetime.datetime(
            *xlrd.xldate_as_tuple(sheet.cell(r, 7).value, dm_file.datemode)))
        consent_date = None if (sheet.cell(r, 9).value == '') else str(datetime.datetime(
            *xlrd.xldate_as_tuple(sheet.cell(r, 9).value, dm_file.datemode)))
        last_participation_date = None if (sheet.cell(r, 10).value == '') else str(datetime.datetime(
            *xlrd.xldate_as_tuple(sheet.cell(r, 10).value, dm_file.datemode)))
        death_date = None if (sheet.cell(r, 11).value == '') else str(datetime.datetime(
            *xlrd.xldate_as_tuple(sheet.cell(r, 11).value, dm_file.datemode)))
        site_id = str(sheet.cell(r, 13).value)
        investigator_id = str(sheet.cell(r, 14).value)
        investigator_name = "Unknown" if (sheet.cell(r, 15).value == '') else str(sheet.cell(r, 15).value)
        dm_birth_date = "Unknown" if (sheet.cell(r, 16).value == '') else str(sheet.cell(r, 16).value)
        dm_age = "Unknown" if (sheet.cell(r, 17).value == '') else str(sheet.cell(r, 17).value)
        dm_age_unit = "Unknown" if (sheet.cell(r, 18).value == '') else str(sheet.cell(r, 18).value)
        dm_sex = str(sheet.cell(r, 19).value)
        dm_race = str(sheet.cell(r, 20).value)
        dm_ethnic = str(sheet.cell(r, 21).value)
        country_name = str(sheet.cell(r, 26).value)
        dm_collection_date = None if (sheet.cell(r, 27).value == '') else str(datetime.datetime(
            *xlrd.xldate_as_tuple(sheet.cell(r, 27).value, dm_file.datemode)))
        dm_therapeutic_area = str(sheet.cell(r, 29).value)
        dm_compound = str(sheet.cell(r, 30).value)

        #Create the Node structure for Study, Site, Subject, Country, Compound, Therapeutic,Investigator,Demographic.

        study_node=Node("Study", name=study_id)
        graph.merge(study_node,"Study")
        compound_node=Node("Compound",name=dm_compound)
        graph.merge(compound_node,"Compound")
        therapeutic_node=Node("Therapeutic", name=dm_therapeutic_area)
        graph.merge(therapeutic_node,"Therapeutic")
        country_node = Node("Country", name=country_name)
        graph.merge(country_node, "Country")
        site_node=Node("Site", name=site_id)
        graph.merge(site_node, "Site")
        subject_node=Node("Subject", name=subject_id, birthdate=dm_birth_date, age=dm_age,age_unit=dm_age_unit, sex=dm_sex, race=dm_race, ethnic=dm_ethnic)
        graph.merge(subject_node, "Subject")
        investigator_node=Node("Investigator", name=investigator_name, id=investigator_id)
        graph.merge(investigator_node, "Investigator")
        demographic_node=Node("Demographic", name=dm_id,  collection_date=dm_collection_date,age_calculated_date=age_calculated_date,first_treatment_date=first_treatment_date,consent_date=consent_date,last_participation_date=last_participation_date,death_date=death_date)
        graph.merge(demographic_node, "Demographic")


        #Create relationship between nodes

        study_country_relationship = Relationship(study_node, "CONDUCTED_IN", country_node)
        graph.merge(study_country_relationship, study_node, country_node)
        study_compound_relationship = Relationship(study_node, "HAS", compound_node)
        graph.merge(study_compound_relationship, study_node, compound_node)
        therapeutic_compound_relationship = Relationship(therapeutic_node, "HAS", compound_node)
        graph.merge(therapeutic_compound_relationship, therapeutic_node,compound_node)
        compound_therapeutic_relationship = Relationship(compound_node, "HAS", therapeutic_node)
        graph.merge(compound_therapeutic_relationship,compound_node, therapeutic_node)
        country_site_relationship = Relationship(country_node, "HAS", site_node)
        graph.merge(country_site_relationship, country_node, site_node)
        site_investigator_relationship = Relationship(site_node, "HAS", investigator_node)
        graph.merge(site_investigator_relationship, site_node, investigator_node)
        site_subject_relationship = Relationship(site_node, "HAS", subject_node)
        graph.merge(site_subject_relationship, site_node, subject_node)
        subject_dm_relationship = Relationship(subject_node,"HAS",demographic_node)
        graph.merge(subject_dm_relationship,subject_node,demographic_node)
        study_subject_relationship = Relationship(study_node,"HAS",subject_node)
        graph.merge(study_subject_relationship,study_node,subject_node)

        graph.create(study_country_relationship | study_compound_relationship | therapeutic_compound_relationship | country_site_relationship | site_investigator_relationship |
                     site_subject_relationship | compound_therapeutic_relationship | subject_dm_relationship | study_subject_relationship)


def main():
    # Connection Information
    graph = Graph("http://neo4j:Root1234@localhost:7474/db/data")
    path = ('C:/Otsuka/Python/Data Files/Export_DMOut1.xls')

    # Source file location to upload
    dm_file = xlrd.open_workbook(path)

    # Create Neo4j graph
    create_neo4jgraph(graph, dm_file)

if __name__ == "__main__": main()