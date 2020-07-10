import re

declare_array = []
cursor_dict_array = []
control_stmt_stack = []
proc_name = ''


def capitalize(in_string):
    in_a_string = False
    out_string = ''
    for r in range(len(in_string)):
        curr_char = in_string[r]
        if curr_char == "'" and not in_a_string:
            in_a_string = True
        elif curr_char == "'" and in_a_string:
            in_a_string = False

        if not in_a_string:
            curr_char = curr_char.upper()
        out_string = out_string + curr_char
    return out_string


def format_code(input_array):
    # Join the array elements in a string by delimiting them with a space
    # then remove the new line characters and strip spaces
    # then split the string into an array of elements delimited by ";"
    # The split in the end makes the elements of the array to be returned as an array in the end
    return ' '.join(input_array).strip("\n").strip().split(";")


def replace_datatypes(in_string):
    # replace varchar2 and char with a string,
    # integer with int64
    a = in_string.replace('varchar2', 'string') \
        .replace(' char', ' string').replace(' integer', ' int64')
    # remove length, scale & precision which are not relevant in Big Query
    return re.sub(r"\([^0-9]*([0-9]+)[^0-9]*\)", "", a)


def build_cursor(in_string_array):
    global cursor_dict_array
    for i in format_code(in_string_array):
        cursor_dict = {}
        if i.upper().find("CURSOR") >= 0:
            cursor_name = i[i.upper().find("CURSOR") + 6:i.upper().find(" IS")].strip().strip("\n")
            cursor_dict["name"] = cursor_name
            cursor_dict["sql"] = i[i.upper().find("SELECT"):]
            cursor_dict_array.append(cursor_dict)


def declare_section(in_string_array):
    global cursor_dict_array
    out_string_array = []
    for i in format_code(in_string_array):
        if i.upper().find("CURSOR") >= 0:
            continue
        # every variable declaration starts with a declare and if a default value is presented,
        # then it precedes a default key word
        else:
            l_string = "declare " + replace_datatypes(i).strip("\n") + ";"
            if l_string.strip() != "declare ;":
                out_string_array.append(l_string.replace(":=", " default "))
    return out_string_array


def build_temp_tables():
    temp_table_array = ["begin \n"]
    for i in cursor_dict_array:
        l_string = "   create temp table " + i["name"] + \
                   "    as " + i["sql"][:i["sql"].upper().find("FROM")] + \
                   "   , row_number() over (order by 1) rn " + \
                   i["sql"][i["sql"].upper().find("FROM"):] + ";"
        temp_table_array.append(l_string)
    temp_table_array.append("\n end;")
    return temp_table_array


def execution_section(in_string_array):
    global proc_name
    out_string_array = []
    for i in format_code(in_string_array):
        formatted_line = i.replace("\n", " ").upper() + ';'

        # A stack data structure maintains the different flow control statements
        # in the input Oracle stored procedure and based on the lexical ends of each,
        # the stack elements will be popped out
        for j in re.findall('(END;|END LOOP;|END IF;|ELIF|ELSE)', formatted_line):
            control_stmt_stack.pop()

        for j in re.findall('(BEGIN |FOR |IF |ELIF |ELSE )', formatted_line):
            control_stmt_stack.append(j)
        # To retrive the stored procedure name so it matches
        # with the last line of the Oracle store procedure

        if formatted_line.find('END ' + proc_name.upper()) >= 0:
            print("Removing from the stack - End procedure")
            control_stmt_stack.pop()

        out_string_array.append(i)

    print(control_stmt_stack)
    return out_string_array


def for_loop_cursor_build(input_array):
    global declare_array
    l_string = ''
    out_string_array = []
    for r in input_array:
        # remove single line comments
        if r.find("--") >= 0:
            l_string = l_string + ' ' + r[:r.find("--") - 1].replace("\n", " ") + ' '
        else:
            l_string = l_string.replace(r'', '') + ' ' + r.replace("\n", " ") + ' '
    # remove multi-line comments
    l_string = re.sub(r'\/\*.*\*\/', '', l_string)

    while l_string.upper().find(" FOR ") >= 0:
        for_declare_variables = ''
        for_iterator = re.finditer(r'( for )', l_string, re.IGNORECASE)
        end_loop_iterator = re.finditer(r' end-loop; ', l_string.replace(' end loop; ', ' end-loop; '), re.IGNORECASE)

        for_array = []
        end_array = []
        for_final = []
        for_loop_array = []
        for i in for_iterator:
            for_array.append(i.start())

        for i in end_loop_iterator:
            end_array.append(i.end())

        merged_array = for_array + end_array
        merged_array.sort()
        for i in merged_array:
            if i in for_array:
                for_final.append(i)
            else:
                for_list = (for_final[-1], i)
                for_loop_array.append(for_list)
                for_final.pop()

        for_loop_array.sort(key=lambda x: x[0])
        while len(for_loop_array) > 1:
            for_loop_array.pop()

        sub_string = l_string[for_loop_array[0][0]:for_loop_array[0][1]]
        cursor_num = for_loop_array[0][0]
        for_row_handle = sub_string[sub_string.upper().find(" FOR ") + 5:sub_string.upper().find(" IN ")].strip()
        for_cursor_name = sub_string[sub_string.upper().find(" IN ") + 4:sub_string.upper().find(" LOOP ")].strip()
        for_cursor_name = for_cursor_name.strip(")").strip("(")
        if for_cursor_name not in [i["name"] for i in cursor_dict_array]:
            dict_for = {"name": 'l_crsr_' + str(cursor_num), "sql": for_cursor_name}
            for_cursor_name = 'l_crsr_' + str(cursor_num)
            cursor_dict_array.append(dict_for)

        var_iterator = re.findall(rf'\s+' + for_row_handle + '[.]{1}[a-zA-Z0-9_]+\s', sub_string, re.MULTILINE)
        for i in var_iterator:
            for_declare_variables = for_declare_variables + for_cursor_name + '_' + i.replace(for_row_handle + '.', '') \
                .strip() + " varchar2(10); "
        for_declare_variables = for_declare_variables + " " + for_cursor_name + "_cnt integer; "
        for_declare_variables = for_declare_variables + " " + for_cursor_name + "_iter integer; "
        for i in for_declare_variables.split(";"):
            declare_array.append(i + ';')
        declare_array.pop();
        for_loop_build = "set " + for_cursor_name + "_cnt = (select count(*) from " + for_cursor_name + ");"
        for_loop_build = for_loop_build + " while (" + for_cursor_name + "_iter <= " + for_cursor_name + \
                         "_cnt) do set ("

        for i in var_iterator:
            for_loop_build = for_loop_build + for_cursor_name + '_' + i.replace(for_row_handle + '.', '').strip() + ','
        for_loop_build = for_loop_build[:-1] + ") = ("

        for_loop_build = for_loop_build + " select as struct "
        for i in var_iterator:
            for_loop_build = for_loop_build + i.replace(for_row_handle + '.', '') + ','
        for_loop_build = for_loop_build[
                         :-1] + " from " + for_cursor_name + " where rn = " + for_cursor_name + "_iter );"

        l_string = l_string[:for_loop_array[0][1] - 10] + " end while; " + l_string[for_loop_array[0][1]:]
        l_string = l_string[:for_loop_array[0][0]] + for_loop_build + l_string[l_string.upper().find(" LOOP ") + 6:]
        l_string = l_string.replace(for_row_handle + '.', '')

    l_string = re.sub(r'([a-zA-Z0-9_]+)\s*\:\=\s*([a-zA-Z0-9_\']+)', r'set \1 = \2', l_string)
    l_string = re.sub(r'([a-zA-Z0-9_]+)\.\s*([\(a-zA-Z0-9_\'\,\s]+\))\s*\;', r'call \1.\2;', l_string)
    for i in l_string.split(";"):
        out_string_array.append(i + ';')

    return out_string_array


def read_file(file_with_path):
    global proc_name, declare_array
    file = open(file_with_path, "r")
    declare_section_ind = False
    execute_section_ind = False
    execute_array = []
    def_array = []
    changed_def_array = []
    while True:
        b = file.readline()
        if not b:
            break
        b = b.replace("||", " || ")
        # Take a note of the stored procedure name
        if b.upper().find("PROCEDURE") >= 0:
            if len(proc_name) == 0:
                proc_name = b[b.upper().find("PROCEDURE") + 9:b.upper().find("(")].strip()
        # start the declare section where the cursors and variables are declared
        if (b.upper().find("DECLARE") >= 0 or b.upper().find(" AS") >= 0) and not declare_section_ind:
            declare_section_ind = True
            def_array.append(b[:b.upper().find(" AS") + 3])
            b = b[b.upper().find(" AS") + 3:]

        # Once the BEGIN keyword is encountered that means the executable section of the procedure began
        if b.upper().find("BEGIN") >= 0 and declare_section_ind:
            declare_section_ind = False
            execute_section_ind = True

        # If its neither declare nor execute section, then it must be the definition section of the procedure

        if not declare_section_ind and not execute_section_ind:
            def_array.append(b)

        # Store the entire declare section in an array and send it to assign_variable function so that it assigns the
        # variables in the big query format
        if declare_section_ind:
            declare_array.append(b)

        if execute_section_ind:
            execute_array.append(b)

    file.close()

    for i in def_array:
        changed_def_array.append(replace_datatypes(i))

    build_cursor(declare_array)
    changed_execute_array = for_loop_cursor_build(execute_array)

    changed_declare_array = declare_section(declare_array)

    print("-- Definition Section")
    for i in changed_def_array:
        print(i)

    print("-- Declaration Section")
    for i in changed_declare_array:
        print(i)

    print("-- Cursor Section")
    for i in build_temp_tables():
        print(i)

    # print("Execution Section")
    # for i in changed_execute_array:
    #     print(i)

    print("-- For loop Section")

    for i in changed_execute_array:
        print(i)


if __name__ == "__main__":
    read_file('/Users/radha/PycharmProjects/Convert/sample.sql')
