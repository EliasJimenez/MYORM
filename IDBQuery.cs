using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace MyORM
{
   public interface IDBQuery
    {

        string ParseTake(string query,int length);
        string ParseOrderBy(string query,string[] fields);
        string ParseOrderByDescending(string query,string[] fields);
        string Sum(string query, string field);
        string Max(string query, string field);
        string Min(string query, string field);

    }
}
