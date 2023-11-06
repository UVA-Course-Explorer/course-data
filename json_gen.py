from sql_helper import SQLHelper
from sis_mappings import *
import json
from datetime import datetime
import os
import pytz


class JSONGenerator():
    def __init__(self, database_path, table_name, strm):
        self.database_path = database_path
        self.table_name = table_name
        self.strm = strm
        self.sql_helper = SQLHelper(self.database_path, self.table_name)
        self.save_dir = 'data/' + str(strm)



    # method to generate main catalog page
    def generate(self):
        # make sure the json directory exists
        if not os.path.exists(self.save_dir):
            os.makedirs(self.save_dir)

        acad_groups = self.sql_helper.get_unique_acad_groups(self.strm)
        data_map = {}
        # all_orgs = []
        for group in acad_groups:   # looping over the school level (e.g. College of Arts and Sciences, E-School)
            dict_key = f"{acad_group_mapping[group]}"
            orgs = []
            print(f"Generating pages for {acad_group_mapping[group]}")
            for org in self.sql_helper.get_acad_orgs_in_acad_group(self.strm, group):   # looping over the department level (e.g. Computer Science, Biology)
                orgs.append({
                        "name": f"{acad_org_mapping[org]}",
                        "abbr": org})
                
                print(f"\t{acad_org_mapping[org]}")
                self.generate_json_for_acad_org(org, self.strm)
            
            
            # get requirements for College of Arts and Sciences
            if group == 'CGAS':
                # loop over the requirements
                for req in new_requirement_mapping:
                    # generate json for each requirement
                    self.generate_json_for_attr(req, self.strm)
                    # add the requirements to the orgs list
                    orgs.append({
                        "name": f"{new_requirement_mapping[req]}",
                        "abbr": req})
                
                # generate json for engagment
                self.generate_json_for_engagement(self.strm)
                orgs.append({
                    "name": f"Engagement",
                    "abbr": 'EGMT'})
                
                
                # generate json for engagment


                # generate json for each requirement

                # add the requirements to the orgs list
            
            orgs.sort(key=lambda x: x['name'])
            data_map[dict_key] = orgs
            # all_orgs += orgs

        # all_orgs.sort(key=lambda x: x['name'])

        # save json object of data_map
        with open(f'{self.save_dir}/latest_sem.json', 'w') as f:
            json.dump(data_map, f, sort_keys=True)
        
        # with open(f'{self.save_dir}/departments.json', 'w') as f:
        #     json.dump(all_orgs, f)

        # generate semester string
        semester = self.strm_to_str(self.strm)

        # Get the current time in GMT (UTC)
        current_time = datetime.utcnow()

        # Calculate the timestamp in seconds
        timestamp_seconds = int(current_time.timestamp())

        # Get the current time in GMT (UTC)
        utc_time = str(datetime.now(pytz.utc))

        metadata = {
            "semester": semester,
            "last_updated": utc_time
        }

        # Write the timestamp to a JSON file
        with open(f'{self.save_dir}/metadata.json', 'w') as f:
            json.dump(metadata, f)






    def convert_time_string(self, original_time):
        if original_time == "":
                    return ""
        # Extract the time part (HH:MM:SS)
        time_part = original_time.split('-')[0]
        # Create a timezone object for Eastern Time (ET) with DST support
        eastern_tz = pytz.timezone('US/Eastern')
        # Convert the time string to a datetime object and localize it to ET
        time_obj = datetime.strptime(time_part, '%H.%M.%S.%f')
        localized_time = eastern_tz.localize(time_obj)
        # Convert to Eastern Time (ET) while considering DST
        est_time = localized_time.astimezone(eastern_tz)
        # Format the time in AM/PM notation
        est_time_formatted = est_time.strftime('%I:%M %p')
        return est_time_formatted
    


    def write_data_dict_to_json(self, data, json_filename):
        # writes data to json (in the form of files like CS.json, AAS.json, etc.)
        with open(json_filename, 'w') as json_file:
            # Write the opening bracket of the JSON array
            json_file.write('{')

            # Iterate through the elements in 'data' and write each one with a newline
            for department in data:
                json.dump(department, json_file)
                json_file.write(': [ \n')
                for course in data[department]:
                    json.dump(course, json_file)
                    if course != data[department][-1]:
                        json_file.write(', \n')

                if department != list(data.keys())[-1]:
                    json_file.write('], \n')
                else:
                    json_file.write('] \n')

            # Write the closing bracket of the JSON array
            json_file.write('}')



    def generate_dict_for_class(self, catalog_number, subject_descr, session_list):
        session_list.sort(key=lambda x: x['display_order'])
        for session in session_list:
            session['meetings'] = eval(session['meetings'])
            for meeting in session['meetings']:
                meeting['start_time'] = self.convert_time_string(meeting['start_time'])
                meeting['end_time'] = self.convert_time_string(meeting['end_time'])
            session['instructors'] = eval(session['instructors'])
        
        class_dict = {
            'catalog_number': catalog_number,
            'subject_descr': subject_descr,
            'subject': session_list[0]['subject'],
            'sessions': session_list,
            'descr': session_list[0]['descr'],
            'topic': session_list[0]['topic'],
            'units': session_list[0]['units'],
            }
        return class_dict



    def generate_json_for_engagement(self, strm):
        data = {'Engagement': []}
        catalog_numbers = self.sql_helper.catalog_numbers_for_subject(strm, 'CGASD', 'Engagement')
        for catalog_number in catalog_numbers:
            session_list = self.sql_helper.get_sessions_for_class_with_org(strm, 'CGASD', 'Engagement', catalog_number)
            class_dict = self.generate_dict_for_class(catalog_number, 'Engagement', session_list)
            data['Engagement'].append(class_dict)
        filename = f'{self.save_dir}/EGMT.json'
        self.write_data_dict_to_json(data, filename)


    def generate_json_for_attr(self, attr, strm):
        subjects = self.sql_helper.get_unique_subject_descr_with_attr(strm, attr)
        data = {subject: [] for subject in subjects}
        for subject_descr in subjects:    # 'Computer Science' is considered a subject
            catalog_numbers = self.sql_helper.get_catalog_numbers_for_subject_with_attr(strm, attr, subject_descr)
            for catalog_number in catalog_numbers:
                session_list = self.sql_helper.get_sessions_for_class(strm, subject_descr, catalog_number)
                class_dict = self.generate_dict_for_class(catalog_number, subject_descr, session_list)
                data[subject_descr].append(class_dict)
        
        # write data to json
        json_filename = f'{self.save_dir}/{attr}.json'
        self.write_data_dict_to_json(data, json_filename)


    def generate_json_for_acad_org(self, acad_org, strm):
        subjects = self.sql_helper.get_unique_subjects_in_org(strm, acad_org)
        data = {subject: [] for subject in subjects}
        for subject_descr in subjects:    # 'Computer Science' is considered a subject
            catalog_numbers = self.sql_helper.catalog_numbers_for_subject(strm, acad_org, subject_descr)
            for catalog_number in catalog_numbers:

                session_list = self.sql_helper.get_sessions_for_class_with_org(strm, acad_org, subject_descr, catalog_number)

                class_dict = self.generate_dict_for_class(catalog_number, subject_descr, session_list)
                data[subject_descr].append(class_dict)
        
        # write data to json
        stripped_acad_org = acad_org.lstrip("\'").strip("\'")
        json_filename = f'{self.save_dir}/{stripped_acad_org}.json'
        self.write_data_dict_to_json(data, json_filename)


    def strm_to_str(self, strm):
        year = str(strm)[1:3]
        season = str(strm)[3:]
        if season == '1':
            season = 'January'
        elif season == '2':
            season = 'Spring'
        elif season == '6':
            season = 'Summer'
        elif season == '8':
            season = 'Fall'
        
        # get the first two digits of the year
        current_year = datetime.now().year
        first_two_digits = str(current_year)[:2]

        return f'{season} {first_two_digits}{year}'




