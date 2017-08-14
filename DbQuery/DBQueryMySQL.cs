using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace MyORM
{
    class DBQueryMySQL : IDBQuery
    {
        public string Max(string query, string field)
        {
            throw new NotImplementedException();
        }

        public string Min(string query, string field)
        {
            throw new NotImplementedException();
        }

        public string ParseOrderBy(string query, string[] fields)
        {
            throw new NotImplementedException();
        }

        public string ParseOrderByDescending(string query, string[] fields)
        {
            throw new NotImplementedException();
        }

        public string ParseTake(string query, int length)
        {
            return $"{query} LIMIT {length}";
        }

        public string Sum(string query, string field)
        {
            throw new NotImplementedException();
        }
    }
}
