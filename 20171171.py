import operator
from collections import defaultdict
from statistics import mean
import sys
import sqlparse
from sqlparse import sql
from sqlparse import tokens as T


attribute_tokens = []
table_tokens = []
condition_tokens = []
logical_op = None
distinct = False
wildcard_star = None
relational_ops = {
    '!=': operator.ne,
    '=': operator.eq,
    '>': operator.gt,
    '>=': operator.ge,
    '<': operator.lt,
    '<=': operator.le,
} # relational_ops[$input](var1, var2) gives bool result
aggregate_ops = {
    'MAX': max,
    'MIN': min,
    'SUM': sum,
    'AVG': mean,
} # aggregate_ops[$input](list)


TABLES = defaultdict(dict)
DIR_PATH = "./files"
JOIN_ATTR_LIST = []


def join(table1, table2):
    """Returns CROSS JOIN of table1 and table2"""

    try:
        joined_table = defaultdict(dict)
        joined_table["attributes"] = table1["attributes"] + table2["attributes"]
        joined_table["rows"] = []
        for row_a in table1["rows"]:
            for row_b in table2["rows"]:
                joined_table["rows"].append(row_a + row_b)
    except Exception as e:
        print("JoinError:", str(e))
        exit(1)
    return joined_table


def get_column_table(token):
    """Returns table_name to which given attribute[token] belongs"""

    global TABLES

    col_tables = []
    for table in [t.value for t in table_tokens]:
        if token.value in [name[len(table) + 1:] for name in TABLES[table]["attributes"]]:
            col_tables.append(table)
    
    if len(col_tables) > 1:
        print("AttributeError: Ambiguous attribute name", token.value + ",", "corresponding table_name not specified")
        exit(1)
    return None if not col_tables else col_tables[0]


def apply_condition(output):
    """Applies conditions given in WHERE clause to resulting output of CROSS JOIN, i.e., filters result"""

    global JOIN_ATTR_LIST, condition_tokens, relational_ops, logical_op

    filtered_output = defaultdict(dict)
    filtered_output["attributes"] = output["attributes"]
    filtered_output["rows"] = []

    for row in output["rows"]:
        should_include = None
        
        for condition_token in condition_tokens:
            var1, var2, op = 3*[None]
            iden_cnt = 0

            try:
                for token in condition_token.tokens:
                    if isinstance(token, sql.Identifier):
                        check_attribute(token)
                        attr_name = token.value if token.get_parent_name() else str(get_column_table(token) + "." + token.get_real_name())
                        if var1 is None:
                            var1 = row[output["attributes"].index(attr_name)]
                        else:
                            var2 = row[output["attributes"].index(attr_name)]
                        if iden_cnt:
                            if attr_name not in JOIN_ATTR_LIST:
                                JOIN_ATTR_LIST.append(attr_name)
                        iden_cnt += 1
                    elif token.ttype is T.Comparison:
                        op = token.value
                    elif token.ttype is T.Number.Integer:
                        if var1 is None:
                            var1 = int(token.value)
                        else:
                            var2 = int(token.value)
            except Exception as e:
                print("ConditionParsingError:", str(e))
                exit(1)

            if op != "=":
                JOIN_ATTR_LIST = []

            truth_val = relational_ops[op](var1, var2)
            if should_include is None:
                should_include = truth_val
                if logical_op is None:
                    break
            else:
                if logical_op == "AND":
                    should_include = should_include and truth_val
                elif logical_op == "OR":
                    should_include = should_include or truth_val
        
        if not condition_tokens:
            should_include = True
        if should_include:
            filtered_output["rows"].append(row)

    return filtered_output


def print_output(filtered_output):
    """Projects given attributes and prints final result"""

    global JOIN_ATTR_LIST, wildcard_star, attribute_tokens, aggregate_ops
    attr_names = []
    idx_list = []

    rem_list = []
    for attr_name in JOIN_ATTR_LIST:
        idx = filtered_output["attributes"].index(attr_name)
        rem_list.append(idx)

    out_rows = []
    if wildcard_star:
        idx_list = range(len(filtered_output["attributes"]))
        attr_names = filtered_output["attributes"]

    for attribute in attribute_tokens:
        if type(attribute) == tuple: # Aggr Function
            attr_name = attribute[1].value if attribute[1].get_parent_name() else str(get_column_table(attribute[1]) + "." + attribute[1].get_real_name())
            idx = filtered_output["attributes"].index(attr_name)

            temp = []
            for row in filtered_output["rows"]:
                temp.append(row[idx])
            
            print(attribute[0].value + "(" + attr_name + ")")
            print(aggregate_ops[attribute[0].value.upper()](temp))
            return

        else:
            attr_name = attribute.value if attribute.get_parent_name() else str(get_column_table(attribute) + "." + attribute.get_real_name())
            if attr_name in JOIN_ATTR_LIST:
                continue
            idx_list.append(filtered_output["attributes"].index(attr_name))
            attr_names.append(attr_name)

    attr_names[:] = (attr_name for attr_name in attr_names if attr_name not in JOIN_ATTR_LIST)

    print(",".join(attr_names))

    for row in filtered_output["rows"]:
        temp_row = []
        for idx in idx_list:
            if idx not in rem_list:
                temp_row.append(row[idx])
        out_rows.append(",".join(map(str, temp_row)))

    if distinct:
        used = set()
        unique = [x for x in out_rows if x not in used and (used.add(x) or True)]
        out_rows = unique

    print(*out_rows, sep="\n")


def add_column(token):
    """Adds attribute to attribute list"""

    global attribute_tokens

    if isinstance(token, sql.Identifier):
        attribute_tokens.append(token)
    if isinstance(token, sql.Function):
        # (function, column)
        attribute_tokens.append((token.tokens[0], token.tokens[1].tokens[1]))


def parser(query):
    """Parses given SQL query"""

    global distinct, wildcard_star, condition_tokens, table_tokens, logical_op

    parsed = sqlparse.parse(query)[0]
    from_seen = False

    for token in parsed.tokens:
        if token.ttype is T.Keyword and token.value.upper() == 'FROM': # FROM
            from_seen = True

        if from_seen:  # Tables List
            if isinstance(token, sql.Identifier):
                table_tokens.append(token)
            if isinstance(token, sql.IdentifierList):
                for identifier in token.get_identifiers():
                    table_tokens.append(identifier)
        else:  # attribute_tokens list
            if isinstance(token, sql.IdentifierList):
                for identifier in token.get_identifiers():
                    add_column(identifier)
            else:
                if token.ttype is T.DML:
                    continue
                if token.ttype is T.Wildcard and token.value == "*":  # SELECT *
                    wildcard_star = True
                elif token.ttype is T.Keyword and token.value.upper() == "DISTINCT": # DISTINCT
                    distinct = True
                else:
                    add_column(token)


        if isinstance(token, sql.Where):  # WHERE
            comp_cnt = 0

            for token_d1 in token.tokens:
                if isinstance(token_d1, sql.Comparison):  # Condition
                    comp_cnt += 1
                    condition_tokens.append(token_d1)
                if token_d1.ttype is T.Keyword and token_d1.value.upper() != "WHERE":
                    if comp_cnt != 1:
                        print("WhereError: Incorrect syntax with logical operator")
                        exit(1)
                    logical_op = token_d1.value.upper()
            
            if logical_op and comp_cnt < 2:
                print("WhereError: Operand missing with logical operator")
                exit(1)
            if comp_cnt > 1 and logical_op is None:
                print("WhereError: Logical operator missing and given multiple conditions")
                exit(1)
            if not condition_tokens:
                print("WhereError: No conditions given")
                exit(1)        


def get_tables_metadata(metadata_path):
    """Fetches and stores table schema from metadata file"""

    global TABLES

    try:
        with open(metadata_path) as metadata_file:
            metadata_file = list(map(str.strip, metadata_file.readlines()))

        try:
            for i in range(len(metadata_file)):
                if metadata_file[i] == "<begin_table>":
                    i += 1
                    table_name = metadata_file[i]
                    TABLES[table_name] = defaultdict(dict)
                    TABLES[table_name]["attributes"] = []
                    i += 1
                    while metadata_file[i] != "<end_table>":
                        TABLES[table_name]["attributes"].append(table_name + "." + metadata_file[i])
                        i += 1
        except Exception as e:
            print("MetadataInputError: " + str(e))

    except Exception as e:
        print("MetadataReadingError: " + str(e))
        

def get_table_data(files_dir, table_name):
    """Fetches and stores table data from csv file given path and filename"""

    global TABLES

    try:
        with open(files_dir + "/" + table_name + ".csv") as table_file:
            table_file = list(map(str.strip, table_file.readlines()))
        
        TABLES[table_name]["rows"] = []
        for line in table_file:
            line = list(map(int, line.split(",")))

            if len(line) != len(TABLES[table_name]["attributes"]):
                print("Inconsistency between metadata file and", table_name + ".csv")
                exit(0)
            else:
                TABLES[table_name]["rows"].append(line)

    except Exception as e:
        print("TabledataReadingError: " + str(e))


def check_attribute(attribute):
    """Handles attribute errors"""

    global aggregate_ops, table_tokens

    if type(attribute) == tuple:
        if attribute[0].value.upper() not in aggregate_ops.keys():
            print("FunctionError:", attribute[0].value + "()", "is invalid")
            exit(1)
        attribute = attribute[1]
    if attribute.get_parent_name() is None:
        if get_column_table(attribute) is None:
            print("AttributeError: Attribute", attribute.value, "does not exist in given table(s)")
            exit(1)
    else:
        if attribute.get_parent_name() not in [t.value for t in table_tokens]:
            print("TableError:", attribute.get_parent_name(), "in", attribute.value, "not in given table list")
            exit(1)


def check_misc_errors():
    """Checks unclassified errors"""

    global attribute_tokens, wildcard_star, logical_op, condition_tokens, table_tokens

    if not attribute_tokens and not wildcard_star:
        print("SyntaxError: No attributes given")
        print("Standard Query: select * from table_name where condition;")
        exit(1)
    if not table_tokens:
        print("SyntaxError: No tables given")
        print("Standard Query: select * from table_name where condition;")
        exit(1)
    if logical_op and not condition_tokens:
        print("SyntaxError: No condition given")
        print("Standard Query: select * from table_name where condition;")
        exit(1)


    # Attribute Checking
    agg_cnt = 0
    for attribute in attribute_tokens:
        check_attribute(attribute)
        if type(attribute) == tuple:
            agg_cnt += 1
    
    # Aggregate Function Checking
    if agg_cnt:
        if agg_cnt > 1:
            print("AttributeError: Aggregate function supported only on one attribute")
            exit(1)
        if len(attribute_tokens) != 1:
            print("AttributeError: Normal attributes not supported with aggregated attributes")
            exit(1)


def check_query_structure(query):
    """Checks query structure for valid queries"""

    query = query.split()

    i = 0
    if query[i].upper() == "SELECT":
        i += 1
        while i < len(query):
            if query[i].upper() == "FROM":
                break
            i += 1
        if i == len(query):
            print("SyntaxError: FROM keyword missing")
            print("Standard Query: select * from table_name where condition;")
            exit(1)
        while i < len(query):
            if ";" in query[i]:
                break
            i += 1
        if i < len(query) - 1:
            print("InvalidQueryError: Only one SQL statement supported")
            exit(1)
        if ";" not in query[-1]:
            print("SyntaxError: Missing semicolon ';' at the end of query")
            exit(1)
    else:
        print("SyntaxError: SELECT keyword", "spelt wrong" if query[i].upper() in "SELECT" else "missing")
        print("Standard Query: select * from table_name where condition;")        
        exit(1)


if __name__ == "__main__":

    if len(sys.argv) != 2:
        print("Usage: python3 20171171.py \"<query_string>\"")
        exit(1)
    
    query = sys.argv[1]

    get_tables_metadata(DIR_PATH + "/" + "metadata.txt")
    for table_name in TABLES.keys():
        get_table_data(DIR_PATH, table_name)

    # print(sqlparse.format(query, reindent=True, keyword_case='upper') + "\n")
    
    check_query_structure(query)
    parser(query)
    check_misc_errors()

    joined_table = None
    for table in table_tokens:
        if joined_table is None:
            joined_table = TABLES[table.value]
        else:
            joined_table = join(joined_table, TABLES[table.value])

    filtered_output = apply_condition(joined_table)
    print_output(filtered_output)
