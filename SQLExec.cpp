/**
 * @file SQLExec.cpp - implementation of SQLExec class
 * @author Kevin Lundeen
 * @see "Seattle University, CPSC5300, Winter 2023"
 */
#include "SQLExec.h"

using namespace std;
using namespace hsql;

// define static data
Tables *SQLExec::tables = nullptr;

// make query result be printable
ostream &operator<<(ostream &out, const QueryResult &qres) {
    if (qres.column_names != nullptr) {
        for (auto const &column_name: *qres.column_names)
            out << column_name << " ";
        out << endl << "+";
        for (unsigned int i = 0; i < qres.column_names->size(); i++)
            out << "----------+";
        out << endl;
        for (auto const &row: *qres.rows) {
            for (auto const &column_name: *qres.column_names) {
                Value value = row->at(column_name);
                switch (value.data_type) {
                    case ColumnAttribute::INT:
                        out << value.n;
                        break;
                    case ColumnAttribute::TEXT:
                        out << "\"" << value.s << "\"";
                        break;
                    default:
                        out << "???";
                }
                out << " ";
            }
            out << endl;
        }
    }
    out << qres.message;
    return out;
}

QueryResult::~QueryResult() {
    // FIXME
        if (column_names != nullptr) {
        delete column_names;
    }
    if (column_attributes != nullptr) {
        delete column_attributes;
    }
    if (rows != nullptr) {
        for (auto row : *rows) {
            delete row;
        }
        delete rows;
    }
}

QueryResult *SQLExec::execute(const SQLStatement *statement) {
    // FIXME: initialize _tables table, if not yet present
     if (SQLExec::tables == nullptr) {
        SQLExec::tables = new Tables();
    }
    try {
        switch (statement->type()) {
            case kStmtCreate:
                return create((const CreateStatement *) statement);
            case kStmtDrop:
                return drop((const DropStatement *) statement);
            case kStmtShow:
                return show((const ShowStatement *) statement);
            default:
                return new QueryResult("not implemented");
        }
    } catch (DbRelationError &e) {
        throw SQLExecError(string("DbRelationError: ") + e.what());
    }
}

void
SQLExec::column_definition(const ColumnDefinition *col, Identifier &column_name, ColumnAttribute &column_attribute) {
   // throw SQLExecError("not implemented");  // FIXME
    column_name = col->name;
    switch(col->type){
        case ColumnDefinition::INT:
            column_attribute.set_data_type(ColumnAttribute::INT);
             break;
         case ColumnDefinition::TEXT:
            column_attribute.set_data_type(ColumnAttribute::TEXT);
             break;    
        default:
            throw SQLExecError("not implemented");                   
    }

}
//Creat table 
QueryResult *SQLExec::create(const CreateStatement *statement) {
    //return new QueryResult("not implemented"); // FIXME
    if(statement->type !=CreateStatement::kTable){
        return new QueryResult(" Error,try to create again");
    }
    ColumnNames colNames;
    ColumnAttribute columnAttribute;
    ColumnAttributes columnAttributes;
    ValueDict row;
    Identifier tableName = statement->tableName;
    Identifier colname;
    // update table schema
    row["table_name"] = tableName;
    Handle tableHandle = SQLExec::tables->insert(&row);
    // Get columns schema
    for(ColumnDefinition* column : *statement->columns){
        column_definition(column, colname, columnAttribute);
        colNames.push_back(colname);
        columnAttributes.push_back(columnAttribute);
    }
    // update columns
    try{
         Handles handles;
        DbRelation& cols = SQLExec::tables->get_table(Columns::TABLE_NAME);
        try{
            for(int i  = 0; i<colNames.size();i++){
                row["column_name"]=colNames[i];
                row["data_type"]= Value(columnAttributes[i].get_data_type()== ColumnAttribute::INT ? "INT" : "TEXT");
                handles.push_back(cols.insert(&row));
            }
            // table relation
            DbRelation& table = SQLExec::tables->get_table(tableName);
            if(statement->ifNotExists){
                table.create_if_not_exists();
            }
            else{
                table.create();
            }
        } catch (exception& e){
           // remove from columns
            try{
                for(unsigned int i = 0 ; i < handles.size();i++){
                    cols.del(handles.at(i));
                }
            }catch(exception& e){}
            throw;
          }
    }catch (exception& e) {
        try {
            // try to remove from _tables
            SQLExec::tables->del(tableHandle);
        } catch(exception& e){}
        throw;
    }
     return new QueryResult("created " + tableName);
}

// DROP ...
QueryResult *SQLExec::drop(const DropStatement *statement) {
   // return new QueryResult("not implemented"); // FIXME
     if (statement->type != DropStatement::kTable) {
        return new QueryResult("Unrecoangized Drop type");
    }
     Identifier tableName = statement->name;
      if (tableName == Tables::TABLE_NAME || tableName == Columns::TABLE_NAME) {
        throw SQLExecError("Cannot drop a schema table!");
    }
    DbRelation& table = SQLExec::tables->get_table(tableName); //Get the table
       //Remove _columns schema table
    ValueDict where;
    where["table_name"] = Value(tableName);
    DbRelation& col = SQLExec::tables->get_table(Columns::TABLE_NAME);
    Handles* handles = col.select(&where);
    for (unsigned int i = 0; i < handles->size(); i++) {
        col.del(handles->at(i));
    }
    delete handles; //Handle memory that was declared in heap
    table.drop(); //drop table
    SQLExec::tables->del(*SQLExec::tables->select(&where)->begin());
    return new QueryResult("dropped " + tableName);
}

QueryResult *SQLExec::show(const ShowStatement *statement) {
   // return new QueryResult("not implemented"); // FIXME
      switch (statement->type) {
    case ShowStatement::EntityType::kTables:
        return show_tables();
    case ShowStatement::EntityType::kColumns:
        return show_columns(statement);
    default:
        throw SQLExecError("not implemented");
    }
}

QueryResult *SQLExec::show_tables() {
   // return new QueryResult("not implemented"); // FIXME
     // Set up labels
    ColumnNames *colNames = new ColumnNames;
    colNames->push_back("table_name");
    ColumnAttributes *colAttr = new ColumnAttributes;
    colAttr->push_back(ColumnAttribute(ColumnAttribute::TEXT));
    
    // Retrieve handles from schema and look up table names
    ValueDicts *tableNames = new ValueDicts();
    
    Handles *handles = SQLExec::tables->select();
  
    unsigned int size = handles->size()-2 ;
   
    for (auto const &h : *handles) {
        ValueDict *row = SQLExec::tables->project(h, colNames);
        Identifier myName = row->at("table_name").s;
        if (myName != Tables::TABLE_NAME && myName != Columns::TABLE_NAME)
            tableNames->push_back(row);
    }
    delete handles;
    return new QueryResult(colNames, colAttr, tableNames,
            "successfully returned " + to_string(size) + " rows");
}

QueryResult *SQLExec::show_columns(const ShowStatement *statement) {
   // return new QueryResult("not implemented"); // FIXME
     DbRelation &Cols = SQLExec::tables->get_table(Columns::TABLE_NAME);

    // Set up labels
    ColumnNames *colNames = new ColumnNames();
    colNames->push_back("table_name");
    colNames->push_back("column_name");
    colNames->push_back("data_type");
    ColumnAttributes *colAttr = new ColumnAttributes();
    colAttr->push_back(ColumnAttribute(ColumnAttribute::TEXT));

    // Retrieve handles for this table from columns schema
    ValueDict where;
    where["table_name"] = Value(statement->tableName);
    Handles *handles = Cols.select(&where);
    unsigned int size = handles->size();
    ValueDicts *rows = new ValueDicts;
    for (auto const &h : *handles) {
        ValueDict* row = Cols.project(h, colNames);
        rows->push_back(row);
    }
    delete handles;
    return new QueryResult(colNames, colAttr, rows,
            "successfully returned " + to_string(size) + " rows");
}

