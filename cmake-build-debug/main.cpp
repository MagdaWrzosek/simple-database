#include <iostream>
#include <fmt/core.h>
#include <string>
#include <vector>
#include <sstream>
#include <fstream>
#include <map>

enum class TableOperator {
    less, more, equals, not_equals
};

enum class WhereOperator {
    OR, AND, NONE
};

enum class TableDataType {
    String, Integer, Float
};

static const char *const datapath = "./data/";

static const char *const metada_path = "./metadata/";

std::string tdt_toString(TableDataType tdt) {
    std::string result;
    switch (tdt) {
        case TableDataType::String:
            result = "String";
            break;
        case TableDataType::Integer:
            result = "Integer";
            break;
        case TableDataType::Float:
            result = "Float";
            break;
            result = "String";
    }
    return result;
}

class Where {
public:
    Where(const std::string &col, TableOperator op, const std::string &val, WhereOperator nextExpressionOperator) : col(
            col), op(op), val(val), next_expression_operator(nextExpressionOperator) {}

    std::string col;
    TableOperator op;
    std::string val;
    WhereOperator next_expression_operator;

};

TableOperator table_operator_from_string(std::string token) {
    if (token == "=")
        return TableOperator::equals;
    if (token == "!=")
        return TableOperator::not_equals;
    if (token == "<")
        return TableOperator::less;

    return TableOperator::more;
}

WhereOperator where_operator_from_string(std::string token) {
    if (token == "AND")
        return WhereOperator::AND;
    if (token == "OR")
        return WhereOperator::OR;
}


class Select {
public:
    Select(const std::vector<std::string> &colums, const std::string &tableName, const std::vector<Where> &wheres)
            : colums(colums), table_name(tableName), wheres(wheres) {}

    std::vector<std::string> colums;
    std::string table_name;
    std::vector<Where> wheres;

};

class Create {
public:
    Create(const std::string &table_name, const std::vector<std::string> columns,
           const std::vector<TableDataType> types)
            : table_name(table_name), columns(columns), types(types) {}

    std::string table_name;
    std::vector<std::string> columns;
    std::vector<TableDataType> types;

    // void handle_cr(std::string table_name, std::vector)
};

class Insert {
public:
    Insert(const std::string table_name, const std::vector<std::string> column, const std::vector<std::string> value)
            : table_name(table_name), column(column), value(value) {}

    std::string table_name;
    std::vector<std::string> column;
    std::vector<std::string> value;
};

class Update {
public:
    Update(const std::string &tableName, const std::vector<std::string> &column, const std::vector<std::string> &value,
           const std::vector<Where> &wheres) : table_name(tableName), column(column), value(value), wheres(wheres) {}

    Update(Update &u) : table_name(u.table_name), column(u.column), value(u.value), wheres(u.wheres) {}

public:
    std::string table_name;
    std::vector<std::string> column;
    std::vector<std::string> value;
    std::vector<Where> wheres;
};

class Drop  {
public:
    Drop (const std::vector<std::string> &colums, const std::string &tableName, const std::vector<Where> &wheres)
            : colums(colums), table_name(tableName), wheres(wheres) {}

    std::vector<std::string> colums;
    std::string table_name;
    std::vector<Where> wheres;

};

class Table {
public:
    Table(const std::string &name, const std::vector<std::string> &columns,
          const std::map<std::string, std::vector<std::string>> &data) : name(name), columns(columns), data(data) {}

    std::string name;
    std::vector<std::string> columns;
    std::map<std::string, std::vector<std::string >> data;
};

Table read_data(std::string table_name) {
    auto file = std::ifstream(datapath + table_name + ".txt");
    std::string line;
    std::vector<std::string> columns;
    std::map<std::string, std::vector<std::string >> data;
    std::getline(file, line);
    std::istringstream iss(line);
    std::string col;
    while (iss >> col) {
        columns.push_back(col);
        data[col] = std::vector<std::string>();
    }


    std::string value;
    int current_column_idx = 0;
    while (file >> value) {
        data[columns[current_column_idx % columns.size()]].push_back(value);
        current_column_idx++;
    }


    return Table(
            table_name,
            columns,
            data
    );

}


class TableColumn {
public:
    TableColumn(const std::string name, TableDataType type) {
        this->type = type;
        this->name = name;
    }

    TableColumn() {
        this->type = TableDataType::Integer;
        this->name = "";
    }

public:
    std::string name;
    TableDataType type;
};

class TableDefinition {
public:
    TableDefinition(const std::string &tableName, const std::map<std::string, TableColumn> &cols) : table_name(
            tableName),
                                                                                                    cols(cols) {}

    std::map<std::string, TableColumn> cols;
    std::string table_name;
};


Select parse_select(const std::vector<std::string> &words);

TableDefinition parse_create(const std::vector<std::string> &words);

Insert parse_insert(const std::vector<std::string> &words);

Update parse_update(std::vector<std::string> &words);

Drop parse_drop(const std::vector<std::string> &words);

bool resolve_wheres(const std::vector<Where> &wheres, Table data_table, TableDefinition td, int current_row);

std::vector<std::string> tokenize(std::string expression) {
    std::istringstream iss(expression);

    // Vector to store individual words
    std::vector<std::string> words;

    // Read each word from the string stream
    std::string word;
    while (iss >> word) {
        words.push_back(word);
    }

    return words;
}

TableDefinition get_table_definition(std::string table_name) {
    std::map<std::string, TableColumn> map;
    std::ifstream file(metada_path + table_name + ".txt");

    std::string column_name, column_type;
    while (file >> column_name && file >> column_type) {

        TableDataType tableDataType;
        if (column_type == "String") {
            tableDataType = TableDataType::String;
        }
        if (column_type == "Integer") {
            tableDataType = TableDataType::Integer;
        }
        if (column_type == "Float") {
            tableDataType = TableDataType::Float;
        }
        map[column_name] = TableColumn(column_name, tableDataType);
    }
    return TableDefinition(
            table_name,
            map
    );
}


std::vector<std::vector<std::string>> handle_query(const Select &query) {
    Table data_table = read_data(std::string(query.table_name));
    TableDefinition td = get_table_definition(query.table_name);
    std::vector<std::vector<std::string>> result;
    // col1, col2
    result.push_back(query.colums);

    int row_count = data_table.data[data_table.columns[0]].size();
    for (
            int current_row = 0;
            current_row < row_count;
            current_row++) {
        bool select_row = resolve_wheres(query.wheres, data_table, td, current_row);

        if (select_row) {
            std::vector<std::string> result_row;
            for (std::string col: query.colums) {
                auto val = data_table.data[col][current_row];
                result_row.push_back(val);
            }
            result.push_back(result_row);
        }
    }

    return result;
}

bool resolve_wheres(const std::vector<Where> &wheres, Table data_table, TableDefinition td, int current_row) {
    bool select_row = true;
    for (int i = 0; i < wheres.size(); i++) {
        Where sub_predicate = wheres[i];
        std::string value_in_col = data_table.data[sub_predicate.col][current_row];
        bool predicate_result;
        if (td.cols[sub_predicate.col].type == TableDataType::Integer) {
            int int_in_col = stoi(value_in_col);
            int val_in_query = stoi(sub_predicate.val);
            if (sub_predicate.op == TableOperator::equals) {
                predicate_result = (int_in_col == val_in_query);
            } else if (sub_predicate.op == TableOperator::not_equals) {
                predicate_result = (int_in_col != val_in_query);
            } else if (sub_predicate.op == TableOperator::less) {
                predicate_result = (int_in_col < val_in_query);
            } else if (sub_predicate.op == TableOperator::more) {
                predicate_result = (int_in_col > val_in_query);
            }
        }

        if (td.cols[sub_predicate.col].type == TableDataType::Float) {
            float float_in_col = stof(value_in_col);
            float val_in_query = stof(sub_predicate.val);
            if (sub_predicate.op == TableOperator::equals) {
                predicate_result = (float_in_col == val_in_query);
            } else if (sub_predicate.op == TableOperator::not_equals) {
                predicate_result = (float_in_col != val_in_query);
            } else if (sub_predicate.op == TableOperator::less) {
                predicate_result = (float_in_col < val_in_query);
            } else if (sub_predicate.op == TableOperator::more) {
                predicate_result = (float_in_col > val_in_query);
            }
        }

        if (td.cols[sub_predicate.col].type == TableDataType::String) {
            std::string str_in_col = value_in_col;
            std::string val_in_query = sub_predicate.val;
            if (sub_predicate.op == TableOperator::equals) {
                predicate_result = (str_in_col == val_in_query);
            } else if (sub_predicate.op == TableOperator::not_equals) {
                predicate_result = (strcmp(str_in_col.c_str(), val_in_query.c_str()) > 0);
            } else if (sub_predicate.op == TableOperator::less) {
                predicate_result = (strcmp(str_in_col.c_str(), val_in_query.c_str()) < 0);
            } else if (sub_predicate.op == TableOperator::more) {
                predicate_result = (str_in_col > val_in_query);
            }
        }
        if (i == 0) {
            select_row = predicate_result;
        } else {
            WhereOperator whereOp = wheres[i - 1].next_expression_operator;
            if (whereOp == WhereOperator::AND) {
                select_row = select_row && predicate_result;
            } else if (whereOp == WhereOperator::OR) {
                select_row = select_row || predicate_result;
            }
        }

    }
    return select_row;
}

void create_handler(const TableDefinition &cr) {
    auto file = std::ofstream(datapath + cr.table_name + ".txt");
    auto file_meta = std::ofstream(metada_path + cr.table_name + ".txt");
    for (auto [col_name, col_definition]: cr.cols) {
        file << col_name << " ";
        file_meta << col_name << " " << tdt_toString(col_definition.type) << "\n";
    }
}

void save_data(Table table) {
    std::ofstream file(datapath + table.name + ".txt");
    for (std::string col: table.columns) {
        file << col + " ";
    }
    file << "\n";

    int record_count = table.data[table.columns[0]].size();
    for (int record_idx = 0; record_idx < record_count; record_idx++) {
        for (std::string col: table.columns) {
            file << table.data[col][record_idx] << " ";
        }
        file << "\n";
    }


}


void handle_insert(const Insert &ins) {
    std::vector<std::vector<std::string>> vec;
    Table table = read_data(ins.table_name);
    for (int i = 0; i < ins.column.size(); i++) {
        table.data[ins.column[i]].push_back(ins.value[i]);
    }
    save_data(table);
}

int handle_update(Update &upd) {
    Table data_table = read_data(std::string(upd.table_name));
    TableDefinition td = get_table_definition(upd.table_name);
    int rows_affected = 0;
    int row_count = data_table.data[data_table.columns[0]].size();
    for (
            int current_row = 0;
            current_row < row_count;
            current_row++) {
        bool select_row = resolve_wheres(upd.wheres, data_table, td, current_row);

        if (select_row) {
            std::vector<std::string> result_row;
            for (int j = 0; j < upd.column.size(); j++) {
                data_table.data[upd.column[j]][current_row] = upd.value[j];
            }
            rows_affected++;
        }
    }

    save_data(data_table);
    return rows_affected;

}
//preliminary version of drop handler
void handle_drop(const Drop  &drop) {
    Table data_table = read_data(std::string(drop.table_name));
    TableDefinition td = get_table_definition(drop.table_name);

    int row_count = data_table.data[data_table.columns[0]].size();
    for (
            int current_row = 0;
            current_row < row_count;
            current_row++) {
        bool select_row = resolve_wheres(drop.wheres, data_table, td, current_row);

        if (select_row) {


        }
        save_data(data_table);
    }
}


    int main() {
        while (true) {
            std::cout << "podaj komende" << std::endl;
            std::string input;
            std::getline(std::cin, input);



            //przykładowe inputy
            // std::string select = "SELECT Col1 Col2 FROM table WHERE Col4 > 4 AND Col3 != 4";
            // std::string create = "CREATE table1 imie String nazwisko String wiek Integer";
            // std::string insert = "INSERT imie Jacek nazwisko Nowak wiek 12 INTO table1";
            // std::string update = "UPDATE imie Andzrej IN table1 WHERE imie = Jacek";

            //std::vector<std::string> tokens = tokenize(select);
            //std::cout << tokens[0] << "\n";
            std::vector<std::string> tokens = tokenize(input);
            std::cout << tokens[0] << "\n";

            if (tokens[0] == "SELECT") {
                Select query = parse_select(tokens);
                auto data = handle_query(query);
                for (int i = 0; i < data.size(); i++) {
                    for (int j = 0; j < data[i].size(); j++) {
                        std::cout << data[i][j] << " ";
                    }
                    std::cout << "\n";
                }

            }
            if (tokens[0] == "CREATE") {
                TableDefinition cr = parse_create(tokens);
                create_handler(cr);

            }
            if (tokens[0] == "INSERT") {
                Insert ins = parse_insert(tokens);
                handle_insert(ins);
            }
            if (tokens[0] == "UPDATE") {
                Update upd = parse_update(tokens);
                int rows_affected = handle_update(upd);
            }
        }

        return 0;
    }


    Select parse_select(const std::vector<std::string> &words) {
        std::vector<std::string> cols;
        int i = 1;
        while (words[i] != "FROM") {
            cols.push_back(words[i]);
            i++;
        }
        i++;
        std::string table_name = words[i];
        std::cout << "Table name " << table_name << "\n";
        i++;
        std::vector<Where> wheres;
        if (words.size() > i && words[i] == "WHERE") {
            while (words.size() > i + 3) {
                i++;
                std::string where_token = words[i];
                std::cout << "WHERE token " << where_token << "\n";
                std::string col = words[i];
                TableOperator tableOperator = table_operator_from_string(words[++i]);
                std::string val = words[++i];
                WhereOperator whereOperator = WhereOperator::NONE;
                if (words.size() > i + 1) {
                    whereOperator = where_operator_from_string(words[++i]);
                }
                wheres.push_back(
                        Where(col,
                              tableOperator,
                              val,
                              whereOperator
                        )
                );

            }

        }
        Select query = Select(
                cols, table_name, wheres
        );
        return query;
    }

    TableDefinition parse_create(const std::vector<std::string> &words1) {
        int i = 1;
        std::string table_name = words1[1];
        i++;
        std::map<std::string, TableColumn> table_data;
        for (i; i < words1.size(); i++) {
            std::string col_name = words1[i];
            i++;
            //conwesja co drugiego słowa na typ
            TableDataType col_type;
            if (words1[i] == "String") {
                col_type = TableDataType::String;
            } else if (words1[i] == "Integer") {
                col_type = TableDataType::Integer;
            } else if (words1[i] == "Float") {
                col_type = TableDataType::Float;
            } else {
                std::cout << "wprowadzono niepoprawny typ" << std::endl;
            }
            table_data[col_name] = TableColumn(col_name, col_type);
        }

        return TableDefinition(
                table_name,
                table_data
        );
    }

    Insert parse_insert(const std::vector<std::string> &words) {
        //words[0]=INSERT
        std::vector<std::string> cols;
        std::vector<std::string> vals;

        for (int i = 1; words[i] != "INTO"; i += 2) {
            cols.push_back(words[i]);
            vals.push_back(words[i + 1]);

        }
        std::string table_name = words[words.size() - 1];

        Insert ins = Insert(table_name, cols, vals);
        return ins;
    }

    Update parse_update(std::vector<std::string> words) {
        std::vector<std::string> cols;
        std::vector<std::string> vals;
        int i = 1;
        for (; words[i] != "IN"; i++) {
            if (i % 2 == 1) {
                cols.push_back(words[i]);
            } else {
                vals.push_back(words[i]);
            }
        }
        i++;
        std::string table_name = words[i];
        i++;
        std::vector<Where> wheres;
        if (words.size() > i && words[i] == "WHERE") {
            while (words.size() > i + 3) {
                i++;
                std::string where_token = words[i];
                std::cout << "WHERE token " << where_token << "\n";
                std::string col = words[i];
                TableOperator tableOperator = table_operator_from_string(words[++i]);
                std::string val = words[++i];
                WhereOperator whereOperator = WhereOperator::NONE;
                if (words.size() > i + 1) {
                    whereOperator = where_operator_from_string(words[++i]);
                }
                wheres.push_back(
                        Where(col,
                              tableOperator,
                              val,
                              whereOperator
                        )
                );

            }

        }
        Update upd = Update(table_name, cols, vals, wheres);
        return upd;

    }
    Drop parse_drop(const std::vector<std::string> &words) {
        std::vector<std::string> cols;
        int i = 1;
        while (words[i] != "FROM") {
            cols.push_back(words[i]);
            i++;
        }
        i++;
        std::string table_name = words[i];
        std::cout << "Table name " << table_name << "\n";
        i++;
        std::vector<Where> wheres;
        if (words.size() > i && words[i] == "WHERE") {
            while (words.size() > i + 3) {
                i++;
                std::string where_token = words[i];
                std::cout << "WHERE token " << where_token << "\n";
                std::string col = words[i];
                TableOperator tableOperator = table_operator_from_string(words[++i]);
                std::string val = words[++i];
                WhereOperator whereOperator = WhereOperator::NONE;
                if (words.size() > i + 1) {
                    whereOperator = where_operator_from_string(words[++i]);
                }
                wheres.push_back(
                        Where(col,
                              tableOperator,
                              val,
                              whereOperator
                        )
                );
            }
        }
        Drop query = Drop(
                cols, table_name, wheres
        );
        return query;
    }


    //TASKS
    // Refine the "drop" function
    //Proprly implement the String data type