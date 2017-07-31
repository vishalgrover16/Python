from py2neo import Graph, Node, Relationship
import xlrd, datetime

def create_neo4jgraph(graph, ae_file):
    sheet = ae_file.sheet_by_index(0)
    graph.schema.create_index("Study", "study_id")
    graph.schema.create_index("Subject", "subject_id")
    graph.schema.create_index("Compound", "ae_compound")
    graph.schema.create_index("Therapeutic","ae_therapeutic_area")
    graph.schema.create_index("AdverseEffect","ae_seq")



    for r in range(1, 200):
        # Extract the required values from the excel sheets and store them.
        # At present the functionality extracts all values from a single excel sheet

        ae_id = str(sheet.cell(r, 0).value)
        study_id = str(sheet.cell(r, 1).value)
        domain = str(sheet.cell(r,2).value)
        subject_id = str(sheet.cell(r,3).value)
        ae_seq = str(sheet.cell(r,4).value)
        ae_group_id = str(sheet.cell(r,5).value)
        ae_reference_id = str(sheet.cell(r, 6).value)
        ae_sp_id = str(sheet.cell(r, 7).value)
        ae_term = str(sheet.cell(r, 8).value)
        ae_modify = str(sheet.cell(r, 9).value)
        ae_llt = str(sheet.cell(r, 10).value)
        ae_decode = str(sheet.cell(r, 12).value)
        ae_hlt = str(sheet.cell(r, 14).value)
        ae_hlgt = str(sheet.cell(r, 16).value)
        ae_category = str(sheet.cell(r, 18).value)
        ae_subcategory = str(sheet.cell(r, 19).value)
        ae_bodysystem = str(sheet.cell(r, 21).value)
        ae_systemorganclass = str(sheet.cell(r, 23).value)
        ae_location = str(sheet.cell(r, 25).value)
        ae_severity = str(sheet.cell(r, 26).value)
        ae_serious_event = str(sheet.cell(r, 27).value)
        ae_action_taken_with_study_treatment = str(sheet.cell(r, 28).value)
        ae_other_action_taken = str(sheet.cell(r, 29).value)
        ae_causality = str(sheet.cell(r, 30).value)
        ae_relationship_to_nonstudy_tratment = str(sheet.cell(r, 31).value)
        ae_pattern = str(sheet.cell(r, 32).value)
        ae_outcome = str(sheet.cell(r, 33).value)
        ae_involves_cancer = str(sheet.cell(r, 34).value)
        ae_birth_defect = str(sheet.cell(r, 35).value)
        ae_disability = str(sheet.cell(r, 36).value)
        ae_results_in_death = str(sheet.cell(r, 37).value)
        ae_prolong_hospitalization = str(sheet.cell(r, 38).value)
        ae_life_threatning = str(sheet.cell(r, 39).value)
        ae_overdose = str(sheet.cell(r, 40).value)
        ae_other_medical_serios_event = str(sheet.cell(r, 41).value)
        ae_other_treatment_given = str(sheet.cell(r, 42).value)
        ae_standard_toxicity_grade = str(sheet.cell(r, 43).value)
        ae_duration = str(sheet.cell(r, 48).value)
        ae_end_relative_reference_period = str(sheet.cell(r, 49).value)
        ae_end_reference_time_point = str(sheet.cell(r, 51).value)

        ae_start_datetime = None if (sheet.cell(r, 44).value == '') else str(datetime.datetime(
            *xlrd.xldate_as_tuple(sheet.cell(r, 44).value, ae_file.datemode)))
        ae_end_datetime = None if (sheet.cell(r, 45).value == '') else str(datetime.datetime(
            *xlrd.xldate_as_tuple(sheet.cell(r, 45).value, ae_file.datemode)))
        ae_study_startday = str(sheet.cell(r, 46).value)
        ae_study_endday = str(sheet.cell(r, 47).value)

        ae_therapeutic_area = str(sheet.cell(r, 52).value)
        ae_compound = str(sheet.cell(r, 53).value)


        #Create the Node structure for Study, Subject, Compound, Therapeutic,AdverseEffects.

        study_node=Node("Study", name=study_id)
        graph.merge(study_node,"Study")
        compound_node=Node("Compound",name=ae_compound)
        graph.merge(compound_node,"Compound")
        therapeutic_node=Node("Therapeutic", name=ae_therapeutic_area)
        graph.merge(therapeutic_node,"Therapeutic")
        subject_node=Node("Subject", name=subject_id)
        graph.merge(subject_node, "Subject")
        adverse_effect_node=Node("AdverseEffect", name=ae_seq, id=ae_sp_id,domain=domain,reported_term=ae_term,modified_reported_term=ae_modify,low_level_term=ae_llt,derived_term=ae_decode,high_level_term=ae_hlt,
                     high_level_group_term=ae_hlgt,category=ae_category,subcategory=ae_subcategory,bodysystem=ae_bodysystem,systemorganclass=ae_systemorganclass,location=ae_location,
                     severity=ae_severity,serious_event=ae_serious_event,action_taken_with_study_treatment=ae_action_taken_with_study_treatment,ae_other_action_taken=ae_other_action_taken,
                     causality=ae_causality,relationship_to_nonstudy_tratment=ae_relationship_to_nonstudy_tratment,pattern=ae_pattern,outcome=ae_outcome,involves_cancer=ae_involves_cancer,
                     birth_defect=ae_birth_defect,disability=ae_disability,results_in_death=ae_results_in_death,prolong_hospitalization=ae_prolong_hospitalization,life_threatning=ae_life_threatning,
                     overdose=ae_overdose,other_medical_serios_event=ae_other_medical_serios_event,other_treatment_given=ae_other_treatment_given,standard_toxicity_grade=ae_standard_toxicity_grade,
                     duration=ae_duration,end_relative_reference_period=ae_end_relative_reference_period,end_reference_time_point=ae_end_reference_time_point)
        graph.merge(adverse_effect_node, "AdverseEffect")



        #Create relationship between nodes

        study_compound_relationship = Relationship(study_node, "HAS", compound_node)
        graph.merge(study_compound_relationship, study_node, compound_node)
        therapeutic_compound_relationship = Relationship(therapeutic_node, "HAS", compound_node)
        graph.merge(therapeutic_compound_relationship, therapeutic_node,compound_node)
        compound_therapeutic_relationship = Relationship(compound_node, "HAS", therapeutic_node)
        graph.merge(compound_therapeutic_relationship,compound_node, therapeutic_node)
        study_subject_relationship = Relationship(study_node, "HAS", subject_node)
        graph.merge(study_subject_relationship,study_node, subject_node)
        subject_ae_relationship = Relationship(subject_node,"HAS",adverse_effect_node,start_datetime=ae_start_datetime,end_datetime=ae_end_datetime,study_startday=ae_study_startday,study_endday=ae_study_endday)
        graph.merge(subject_ae_relationship,subject_node,adverse_effect_node)


        graph.create(study_compound_relationship | therapeutic_compound_relationship | subject_ae_relationship | compound_therapeutic_relationship | study_subject_relationship)


def main():
    # Connection Information
    graph = Graph("http://neo4j:Root1234@localhost:7474/db/data")
    path = ('C:/Otsuka/Python/Data Files/Export_AEOut1.xls')

    # Source file location to upload
    ae_file = xlrd.open_workbook(path)

    # Create Neo4j graph
    create_neo4jgraph(graph, ae_file)

if __name__ == "__main__": main()