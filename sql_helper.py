import sqlite3

class SQLHelper:
    def __init__(self, path_to_db='data.db', table_name='sessions'):
        self.path_to_db = path_to_db
        self.table_name = table_name

    def get_unique_acad_groups(self, strm):
        # get the unique acad_groups
        query = f"SELECT DISTINCT acad_group FROM {self.table_name} WHERE strm={strm}"
        conn = sqlite3.connect(self.path_to_db)
        cursor = conn.execute(query)
        unique_acad_groups = cursor.fetchall()
        conn.close()
        return [row[0] for row in sorted(unique_acad_groups)]
    


    def get_catalog_numbers_for_subject_with_attr(self, strm, attr, subject_descr):
        # get the unique catalog numbers for a subject_descr (a specific class)
        subject_descr = subject_descr.replace("'", "''")

        if subject_descr[0] != "'":
            subject_descr = f"'{subject_descr}'"
        
        # code to get unique catalog numbers
        conn = sqlite3.connect(self.path_to_db)
        query = f"""
        SELECT DISTINCT catalog_nbr
        FROM {self.table_name}
        WHERE strm={strm} AND subject_descr={subject_descr} AND crse_attr_value LIKE '%{attr}%';"""
        cursor = conn.execute(query)
        unique_catalog_numbers = cursor.fetchall()
        conn.close()
        return [row[0] for row in sorted(unique_catalog_numbers)]


    def get_unique_subject_descr_with_attr(self, strm, attr):
        # get the unique subject_descrs with a specific attribute
        query = f"""SELECT DISTINCT subject_descr FROM sessions WHERE crse_attr_value LIKE ? AND strm = ?;"""        
        conn = sqlite3.connect(self.path_to_db)
        cursor = conn.execute(query, ('%' + attr + '%', strm))
        results = cursor.fetchall()
        conn.close()
        return [row[0] for row in sorted(results)]


    def get_acad_orgs_in_acad_group(self, strm, acad_group):
        # get academic organizations in academic group (RELA in RELG, for example)
        query = f"SELECT DISTINCT acad_org FROM {self.table_name} WHERE strm={strm} AND acad_group='{acad_group}'"
        conn = sqlite3.connect(self.path_to_db)
        cursor = conn.execute(query)
        unique_acad_orgs = cursor.fetchall()
        conn.close()
        return [row[0] for row in sorted(unique_acad_orgs)]


    def get_unique_acad_orgs(self, strm):
        # query = f"SELECT DISTINCT acad_org FROM sessions WHERE strm={strm}"
        query = f"SELECT DISTINCT acad_org FROM {self.table_name} WHERE strm={strm}"
        conn = sqlite3.connect(self.path_to_db)
        cursor = conn.execute(query)
        unique_subject_descr = cursor.fetchall()
        conn.close()
        return [row[0] for row in sorted(unique_subject_descr)]


    def get_unique_subjects_in_org(self, strm, acad_org):
        if acad_org[0] != "'":
            acad_org = f"'{acad_org}'"
        query = f"SELECT DISTINCT subject_descr FROM {self.table_name} WHERE strm={strm} AND acad_org={acad_org}"
        conn = sqlite3.connect(self.path_to_db)
        cursor = conn.execute(query)
        unique_subject_descr = cursor.fetchall()
        conn.close()
        return [row[0] for row in sorted(unique_subject_descr)]


    def catalog_numbers_for_subject(self, strm, acad_org, subject_descr):
        # get the unique catalog numbers for a subject_descr (a specific class)
        subject_descr = subject_descr.replace("'", "''")

        if subject_descr[0] != "'":
            subject_descr = f"'{subject_descr}'"
        
        if acad_org[0] != "'":
            acad_org = f"'{acad_org}'"
        
        # code to get unique catalog numbers
        conn = sqlite3.connect(self.path_to_db)
        query = f"""
        SELECT DISTINCT catalog_nbr
        FROM {self.table_name}
        WHERE strm={strm} AND subject_descr={subject_descr} AND acad_org={acad_org}"""
        cursor = conn.execute(query)
        unique_catalog_numbers = cursor.fetchall()
        conn.close()
        return [row[0] for row in sorted(unique_catalog_numbers)]


    def get_sessions_for_class_with_org(self, strm, acad_org, subject_descr, catalog_nbr):
        # get all the sessions for a class
        subject_descr = subject_descr.replace("'", "''")

        if subject_descr[0] != "'":
            subject_descr = f"'{subject_descr}'"
        if acad_org[0] != "'":
            acad_org = f"'{acad_org}'"
        
        if type(catalog_nbr) == str and catalog_nbr[0] != "'":
            catalog_nbr = f"'{catalog_nbr}'"
        query = f"""SELECT meetings, subject, section_type, instructors, topic, enrollment_total, class_capacity, wait_tot, wait_cap, subject, catalog_nbr, class_section, descr, units,
        CASE
            WHEN section_type = 'Lecture' THEN 1
            ELSE 2
        END AS display_order
        FROM {self.table_name} 
        WHERE acad_org={acad_org} AND subject_descr={subject_descr} AND strm={strm} AND catalog_nbr={catalog_nbr} ORDER BY component;"""
        conn = sqlite3.connect(self.path_to_db)
        cursor = conn.execute(query)
        column_names = [desc[0] for desc in cursor.description]
        result = [dict(zip(column_names, row)) for row in cursor.fetchall()]        
        conn.close()
        return result
    

    def get_sessions_for_class(self, strm, subject_descr, catalog_nbr):
        # get all the sessions for a class
        subject_descr = subject_descr.replace("'", "''")

        if subject_descr[0] != "'":
            subject_descr = f"'{subject_descr}'"
        
        if type(catalog_nbr) == str and catalog_nbr[0] != "'":
            catalog_nbr = f"'{catalog_nbr}'"
        query = f"""SELECT meetings, subject, section_type, instructors, topic, enrollment_total, class_capacity, wait_tot, wait_cap, subject, catalog_nbr, class_section, descr, units,
        CASE
            WHEN section_type = 'Lecture' THEN 1
            ELSE 2
        END AS display_order
        FROM {self.table_name} 
        WHERE subject_descr={subject_descr} AND strm={strm} AND catalog_nbr={catalog_nbr} ORDER BY component;"""
        conn = sqlite3.connect(self.path_to_db)
        cursor = conn.execute(query)
        column_names = [desc[0] for desc in cursor.description]
        result = [dict(zip(column_names, row)) for row in cursor.fetchall()]
        conn.close()
        return result