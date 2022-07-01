using System;
using System.Collections.Generic;
using Datalite.Destination;
using Datalite.Testing;
using FluentAssertions;

namespace Datalite.Sources.Databases.Odbc.Tests.Integration
{
    internal static class Expectations
    {
        #region Table
        internal static SqliteColumn IdColumn => new("id", StoragesClasses.StorageClassType.IntegerClass, false);
        internal static SqliteColumn FirstNameColumn => new("first_name", StoragesClasses.StorageClassType.TextClass, false);
        internal static SqliteColumn LastNameColumn => new("last_name", StoragesClasses.StorageClassType.TextClass, false);
        internal static SqliteColumn EmailColumn => new("email", StoragesClasses.StorageClassType.TextClass, false);
        internal static SqliteColumn GenderColumn => new("gender", StoragesClasses.StorageClassType.TextClass, false);
        internal static SqliteColumn ImageColumn => new("image", StoragesClasses.StorageClassType.BlobClass, false);
        internal static SqliteColumn SalaryColumn => new("salary", StoragesClasses.StorageClassType.NumericClass, false);

        internal static SqliteColumn[] AllColumns => new[]
        {
            IdColumn,
            FirstNameColumn,
            LastNameColumn,
            EmailColumn,
            GenderColumn,
            ImageColumn,
            SalaryColumn
        };

        internal static Dictionary<string, object> TableRecord1 = new()
        {
            { "id", 1L },
            { "first_name", "Shoshana" },
            { "last_name", "Weinham" },
            { "email", "sweinham0@cafepress.com" },
            { "gender", "Female" },
            {
                "image",
                Convert.FromBase64String("iVBORw0KGgoAAAANSUhEUgAAABAAAAAQCAYAAAAf8/9hAAAABGdBTUEAAK/INwWK6QAAABl0RVh0U29mdHdhcmUAQWRvYmUgSW1hZ2VSZWFkeXHJZTwAAALNSURBVDjLjZBbSNNRHMdn0x7qQXwoyoqYS3tIJUsR8UHnFEKayXCO8Dp0mje8zLVm3vJSGpk6539azQteZt6aYmuKoQ8q3uY9LSJHqFsUJGpqpNu3v0FmGOaBLwfOOZ/P+fGlAKAclFQ/S960zHplse4M5qtoW5XxVnIfZ6ujv+8PhOO9zzK1NTTDUv1p7E15DL3oUAJ5yAnJUqsjdGo+NgYLoGtxxZLiHGZk9OVDCQg/8wL9hBor/QS2JqqxNkBgcVyFnmwH7aEEbG//iNnaJOPqZDMMc61Yn1FC3ytBVrB7yz8FEQxrXinPY0USxAARytyK4tqNSkQxP/QtAiy03YO+PQ19ZbGw519SOQltj/8lCHG9wCzleRqKAhjYm4rCeJSU5yBfmoaMR0K0valFbk8W7NIvVl0W2BzbFRRyr/Q9ifJFa2YYZutTUSfgQBLERFmkJyonUiEbE+GxJhF5miQ0vZUjQRkHlygnYlcg5dKb3vco9hWmSGGgcOwO8jVCZI8k4O5INMRD0eARoZgnfP5fWH62H6TjYigIFroCLdHNNEUb2xwPYh2ge3r9j+DI1WKxVBy3rzBbTjKKM90wnuyCzZcFMM6qsd6QhOEYe+MA73z1L9jEtSGcLdCs9X4C7je2IK1CAaGkCs+GNyGULqKRabG6QcKQsACRBfmIhi8P3dHpSZ2n0LzLOJz4jvX2OQNyZgH+MBDcC/h3AFwFyfQD3R5mMGpasXctZ5wiz02NlKkcx+3R5hIIXugRogZClEYEKIzgVBnAkgGsEgNUN07imzwMIKHvtyn4SubjLSo6vaiLFG2xm645NxE30wcR0QXwXpETKIGgdiBwZ4q8eVTzfTHEt4FORMNnsRk+hJvgNct0W+1FTaG8q4l0UWWxU5w5tUrHhBEw0qfITIOROgm3FA3o155rS5LDM0di7RvIH7U7Y5P7wg68099P+T6oezGZAe0AAAAASUVORK5CYII=")
            },
            { "salary", 19000.28D }
        };

        internal static Dictionary<string, object> TableRecord2 = new()
        {
            { "id", 2L },
            { "first_name", "Coraline" },
            { "last_name", "Hundal" },
            { "email", "chundal1@webeden.co.uk" },
            { "gender", "Female" },
            {
                "image",
                Convert.FromBase64String("iVBORw0KGgoAAAANSUhEUgAAABAAAAAQCAYAAAAf8/9hAAAABGdBTUEAAK/INwWK6QAAABl0RVh0U29mdHdhcmUAQWRvYmUgSW1hZ2VSZWFkeXHJZTwAAAKqSURBVDjLfVNdSBRRFP7mZ2dz3dwfx3VZTVuRiizMwBAiJCixh34gEwLfiqCXegl66D2kB1mJygcjevO1FGKFUhMKI5fZNFYlV31wN7F2nZVk52dnuveuKxjaHc49d2bu953v3HMuZ9s2/jdisdgr0zR7DMMQiQfxzHRdp36I248gHo97CGDQ6/V2OZ0uaAQEywLbTTA+nxfR6DuIe4EVRemkYFmWa8rLy7G0vAoa3bJsWLZFiGwUCJmmaRATiYRWKBQkYmxTSSb12Wz2X8nIZLKEyMLFjkvsm0jBtbV12NhQWXSbPjZVaTOjLyQm/D4Pht++QdOJFhad/mMKKCsFPxyMkW0cOI7MxYl5apu5HJ7caYWPn0coNQrBfRJ/Pk9AthrATpZGpRt9/krwPA+OGL9tdE0JxM33aKsDyuQeeMIt2Fg6CjX6uqiAnAkh4CEIwi6Ckj8dmIXf5YA7eBaZxQQkTsfBiiA8laEiAS0Px3MQRHE3mKg6XKbgZhsHb/gCtPQQJBeHFWUOhilgJOmGSE+SHghNgScKwgEXy5+OKnxFZ2MCnsYryK8OgJdMONz1EPNJ5I/dQ3Z2tEjAb+dJbXGh2AuNkoLu6yDgawT8ArzDhJ4LY+3jNJKV3Qi4a4plpJPDIaL31ilWHsMwkVsZQ3WZhUDTZeg/ByFINvJqPdITX/AjeBuGUMFU0/RFUsu+8fEP7aVGsdem6tvPhGTDrkF65il8AQn5zCGkJmOYdV3FkYbjTGGwuoo126670N/ffz+kDUdu3H2J+aEHSC1/grOuGVtrqjG11ez4pTl3upQaacK5nbsQiUSqSBkjC8l16N9HEG4+B3X9N5LTCdsvV7Q+6n0e3+ve8KUFSSGjqur0t7kU+gaeYXIsihXODeXA+ZmOx/H4ftf9L53Qf7mz5LNnAAAAAElFTkSuQmCC")
            },
            { "salary", 38185.39D }
        };

        internal static Dictionary<string, object> TableRecord3 = new()
        {
            { "id", 3L },
            { "first_name", "Calv" },
            { "last_name", "Murton" },
            { "email", DBNull.Value },
            { "gender", "Non-binary" },
            {
                "image",
                Convert.FromBase64String("iVBORw0KGgoAAAANSUhEUgAAABAAAAAQCAYAAAAf8/9hAAAABGdBTUEAAK/INwWK6QAAABl0RVh0U29mdHdhcmUAQWRvYmUgSW1hZ2VSZWFkeXHJZTwAAAHYSURBVDjLlVLPS1RxHJynpVu7KEn0Vt+2l6IO5qGCIsIwCPwD6hTUaSk6REoUHeoQ0qVAMrp0COpY0SUIPVRgSl7ScCUTst6zIoqg0y7lvpnPt8MWKuuu29w+hxnmx8dzzmE5+l7mxk1u/a3Dd/ejDjSsII/m3vjJ9MF0yt93ZuTkdD0CnnMO/WOnmsxsJp3yd2zfvA3mHOa+zuHTjy/zojrvHX1YqunAZE9MlpUcZAaZQBNIZUg9XdPBP5wePuEO7eyGQXg29QL3jz3y1oqwbvkhCuYEOQMp/HeJohCbICMUVwr0DvZcOnK9u7GmQNmBQLJCgORxkneqRmAs0BFmDi0bW9E72PPda/BikwWi0OEHkNR14MrewsTAZF+lAAWZEH6LUCwUkUlntrS1tiG5IYlEc6LcjYjSYuncngtdhakbM5dXlhgTNEMYLqB9q49MKgsPjTBXntVgkDNIgmI1VY2Q7QzgJ9rx++ci3ofziBYiiELQEUAyhB/D29M3Zy+uIkDIhGYvgeKvIkbHxz6Tevzq6ut+ANh9fldetMn80OzZVVdgLFjBQ0tpEz68jcB4ifx3pQeictVXIEETnBPCKMLEwBIZAPJD767V/ETGwsjzYYiC6vzEP9asLo3SGuQvAAAAAElFTkSuQmCC")
            },
            { "salary", 22776.57D }
        };

        internal static Dictionary<string, object> TableRecord4 = new()
        {
            { "id", 4L },
            { "first_name", "Toinette" },
            { "last_name", "Reedshaw" },
            { "email", DBNull.Value },
            { "gender", "Female" },
            {
                "image",
                Convert.FromBase64String("iVBORw0KGgoAAAANSUhEUgAAABAAAAAQCAYAAAAf8/9hAAAABGdBTUEAAK/INwWK6QAAABl0RVh0U29mdHdhcmUAQWRvYmUgSW1hZ2VSZWFkeXHJZTwAAAJFSURBVDjLpZNfaM0BFMc/v7s/pv25tnsXlrzo2nav3d3y4CaelqLw4l1JEVaUl1GeUHtQXpiSUvKqZFFWJtFajO62K/KwlXQn7syfe+3PPX883AkNKefl1KnzOed8zzmBu/M/Vvm74OnMiayZJlTNVfXO2fT5nX8ChJYm9zRhJFrrWok1xAJRTf+tgyWAU6neDwuyUCx5ieJCEREZ+xsgcHfOjJ50M0XV0LL39sa2QEwYnRr7JKKqqiER4cru641LNFBT1tfGMDfMHccCNcMd4s3xsLribjyeePp7EVUVdcPcyBVe83HuI+KCuRMKKjBz1oVjiMgfAKJk81kaqsKsrG3h/dc86loex+dRUwQlUhdhz7VdLiKIKLcPDATBz3dwbPCgx5vjZKczqBnmirihrjRUhVlTvxYzxzEGRx5w99Bg8MsdiCjqimjZ62KymmIz87x5+YLZ2SLF+QJuxR8jHL13wEWUFTUrUDNKXiprYoqYUZ13ossr2Lh1E2uaYtx/fpPh7EPS3S3nQt8rJ1a2syq8isnPE8SbkiSakiQiKTqiKWSqSKqtEw0pnau3oUGJdMdmgCOVACURBCXz7hkbop1MvJ0kl59CVYmGo8x8zlMV1LGjfT8Ax7su0z/eB9yqqQSQkqBmJCJJRI1cfoobe/sDgO2XurxQmOZ5bojR3CN6tl2ld2AfNRXLAObKABGevBpBVFlc0dwPYcWorw2Gx4aCzckt9I/3UR1U8ijzAOBi8K/vnO5u6QUOA/XAF6Bv+EKu5xvVXGolRpHH+AAAAABJRU5ErkJggg==")
            },
            { "salary", 28294.83D }
        };

        internal static string[] PrimaryIndex = { "id" };
        internal static string[] SecondaryIndex = { "last_name", "gender" };
        #endregion

        #region Query
        internal static SqliteColumn QueryIdColumn => new("id", StoragesClasses.StorageClassType.IntegerClass, false);
        internal static SqliteColumn QueryFirstNameColumn => new("first_name", StoragesClasses.StorageClassType.TextClass, false);
        internal static SqliteColumn QueryLastNameColumn => new("last_name", StoragesClasses.StorageClassType.TextClass, false);
        internal static SqliteColumn QueryEmailColumn => new("email", StoragesClasses.StorageClassType.TextClass, false);

        internal static SqliteColumn[] QueryColumns => new[]
        {
            QueryIdColumn,
            QueryFirstNameColumn,
            QueryLastNameColumn,
            QueryEmailColumn
        };

        internal static Dictionary<string, object> QueryRecord1 = new()
        {
            { "id", 1L },
            { "first_name", "Shoshana" },
            { "last_name", "Weinham" },
            { "email", "sweinham0@cafepress.com" }
        };

        internal static Dictionary<string, object> QueryRecord2 = new()
        {
            { "id", 2L },
            { "first_name", "Coraline" },
            { "last_name", "Hundal" },
            { "email", "chundal1@webeden.co.uk" }
        };

        internal static Dictionary<string, object> QueryRecord3 = new()
        {
            { "id", 3L },
            { "first_name", "Calv" },
            { "last_name", "Murton" },
            { "email", DBNull.Value }
        };

        internal static string[] QueryIndex = { "email" };
        #endregion

        internal static AndConstraint<SqliteTableAssertions> MeetTheTableConditions(
            this SqliteTableAssertions tableAssertions)
        {
            var table = tableAssertions.Table;

            return table
                .Should()
                .Exist().And
                .HaveAColumnCountOf(AllColumns.Length).And
                .HaveColumns(AllColumns).And
                .HaveARowCountOf(1000).And
                .HaveAnIndexOf(PrimaryIndex).And
                .HaveAnIndexOf(SecondaryIndex).And
                .HaveARowMatching(TableRecord1).And
                .HaveARowMatching(TableRecord2).And
                .HaveARowMatching(TableRecord3).And
                .HaveARowMatching(TableRecord4);
        }

        internal static AndConstraint<SqliteTableAssertions> MeetTheQueryConditions(
            this SqliteTableAssertions tableAssertions)
        {
            var table = tableAssertions.Table;

            return table
                .Should()
                .Exist().And
                .HaveAColumnCountOf(QueryColumns.Length).And
                .HaveColumns(QueryColumns).And
                .HaveARowCountOf(3).And
                .HaveAnIndexOf(QueryIndex).And
                .HaveARowMatching(QueryRecord1).And
                .HaveARowMatching(QueryRecord2).And
                .HaveARowMatching(QueryRecord3);
        }
    }
}
