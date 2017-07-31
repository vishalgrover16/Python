from py2neo import Graph, Node, Relationship
import xlrd
graph = Graph("http://neo4j:Root1234@localhost:7474/db/data")
path = ('C:/Users/vrajgopal/Desktop/RBM/Source_files/Python_Input_Files_DONT_TOUCH/pvtable.xls')

pv_file = xlrd.open_workbook(path)
sheet = pv_file.sheet_by_index(0)

for r in range(1, sheet.nrows):

    # Extract the required values from the excel sheets and store them.
    # At present the functionality extracts all values from a single excel sheet
    study_id=sheet.cell(r,1).value.split('-')[-1]
    country_name = sheet.cell(r, 2).value
    site_id=sheet.cell(r,3).value
    subject_id=sheet.cell(r,4).value
    subject_initals=sheet.cell(r,7).value
    subject_age=sheet.cell(r,8).value
    subject_gender=sheet.cell(r,9).value
    sae_id = sheet.cell(r, 0).value
    sae_case_number=sheet.cell(r,5).value
    sae_case_type=sheet.cell(r,6).value
    sae_initial_receipt_date=sheet.cell(r,10).value
    sae_pv_receipt_date=sheet.cell(r,11).value
    sae_description_reported=sheet.cell(r,14).value
    sae_preferred_term=sheet.cell(r,15).value
    sae_event_received_date=sheet.cell(r,16).value
    sae_soc=sheet.cell(r,17).value
    sae_event_onset_date=sheet.cell(r,18).value
    sae_case_seriousness=sheet.cell(r,19).value
    #sae_seriousness_criteria=sheet.cell(r,18).value
    sae_primary_suspect_product=sheet.cell(r,20).value
    sae_study_medication=sheet.cell(r,21).value.split(')')[-1].lstrip()
    sae_as_reported_casualty=sheet.cell(r,22).value.split(')')[-1].lstrip()
    #sae_as_determined_casualty=sheet.cell(r,22).value.split(')')[-1].lstrip()
    sae_outcome=sheet.cell(r,23).value
    sae_case_level_casuality=sheet.cell(r,24).value
    sae_susar=sheet.cell(r,25).value
    sae_daysToNotification=sheet.cell(r,26).value

    # Create the Node structure for Study, Site, Subject, Country, SAE (Severe Adverse Event). The nodes should be unique.
    study_node = Node("Study", name=study_id)
    graph.merge(study_node, "Study")
    site_node = Node("Site", name=site_id)
    graph.merge(site_node, "Site")
    subject_node = Node("Subject", name=subject_id, initals=subject_initals, age=subject_age, gender=subject_gender)
    graph.merge(subject_node, "Subject")
    country_node = Node("Country", name=country_name)
    graph.merge(country_node, "Country")
    sae_node = Node("SAE", name=sae_id, sae_case_number=sae_case_number, type=sae_case_type,
                    initial_receipt_date=sae_initial_receipt_date, pv_receipt_date=sae_pv_receipt_date,
                    description=sae_description_reported, preferred_term=sae_preferred_term,
                    event_received_date=sae_event_received_date, soc=sae_soc,
                    event_onset_date=sae_event_onset_date, case_seriousness=sae_case_seriousness,
                    primary_suspect_product=sae_primary_suspect_product, study_medication=sae_study_medication,
                    as_reported_casualty=sae_as_reported_casualty,
                    outcome=sae_outcome, case_level_casualty=sae_case_level_casuality, susar=sae_susar, daysToNotification=sae_daysToNotification)
    graph.merge(sae_node, "SAE")

    #Create the relationship between nodes and should not be repetative
    #subject_study_relationship = Relationship(subject_node, "ENROLLED", study_node)
    #graph.merge(subject_study_relationship, subject_node, study_node)
    subject_sae_relationship = Relationship(subject_node, "HAS", sae_node)
    graph.merge(subject_sae_relationship, subject_node, sae_node)
    study_country_relationship = Relationship(study_node, "CONDUCTED_IN", country_node)
    graph.merge(study_country_relationship, study_node, country_node)
    country_site_relationship = Relationship(country_node, "HAS", site_node)
    graph.merge(country_site_relationship, country_node, site_node)
    site_subject_node = Relationship(site_node, "HAS", subject_node)
    graph.merge(site_subject_node, site_node, subject_node)
    graph.create(subject_sae_relationship | study_country_relationship | country_site_relationship | site_subject_node)
    #print sae_case_number, sae_case_type, study_id, site_id, subject_id, subject_initals, subject_age, subject_gender, country_name, sae_initial_receipt_date, sae_pv_receipt_date, sae_description_reported
    #print sae_preferred_term, sae_event_received_date, sae_soc, sae_event_onset_date, sae_case_seriousness, sae_primary_suspect_product, sae_study_medication, sae_as_reported_casualty
    #print sae_as_determined_casualty, sae_outcome, sae_case_level_casuality, sae_susar






