using System;
using Datalite.Destination;
using FluentAssertions;

namespace Datalite.Testing
{
    public static class TestData
    {
        public static string QuerySql => "SELECT id, first_name, last_name, email FROM TestData WHERE Id <= 3";

        // Converted via https://cryptii.com/pipes/hex-to-base64
        public static byte[] Row1ImageBinary => Convert.FromBase64String(
            "iVBORw0KGgoAAAANSUhEUgAAABAAAAAQCAYAAAAf8/9hAAAABGdBTUEAAK/INwWK6QAAABl0RVh0U29mdHdhcmUAQWRvYmUgSW1hZ2VSZWFkeXHJZTwAAALNSURBVDjLjZBbSNNRHMdn0x7qQXwoyoqYS3tIJUsR8UHnFEKayXCO8Dp0mje8zLVm3vJSGpk6539azQteZt6aYmuKoQ8q3uY9LSJHqFsUJGpqpNu3v0FmGOaBLwfOOZ/P+fGlAKAclFQ/S960zHplse4M5qtoW5XxVnIfZ6ujv+8PhOO9zzK1NTTDUv1p7E15DL3oUAJ5yAnJUqsjdGo+NgYLoGtxxZLiHGZk9OVDCQg/8wL9hBor/QS2JqqxNkBgcVyFnmwH7aEEbG//iNnaJOPqZDMMc61Yn1FC3ytBVrB7yz8FEQxrXinPY0USxAARytyK4tqNSkQxP/QtAiy03YO+PQ19ZbGw519SOQltj/8lCHG9wCzleRqKAhjYm4rCeJSU5yBfmoaMR0K0valFbk8W7NIvVl0W2BzbFRRyr/Q9ifJFa2YYZutTUSfgQBLERFmkJyonUiEbE+GxJhF5miQ0vZUjQRkHlygnYlcg5dKb3vco9hWmSGGgcOwO8jVCZI8k4O5INMRD0eARoZgnfP5fWH62H6TjYigIFroCLdHNNEUb2xwPYh2ge3r9j+DI1WKxVBy3rzBbTjKKM90wnuyCzZcFMM6qsd6QhOEYe+MA73z1L9jEtSGcLdCs9X4C7je2IK1CAaGkCs+GNyGULqKRabG6QcKQsACRBfmIhi8P3dHpSZ2n0LzLOJz4jvX2OQNyZgH+MBDcC/h3AFwFyfQD3R5mMGpasXctZ5wiz02NlKkcx+3R5hIIXugRogZClEYEKIzgVBnAkgGsEgNUN07imzwMIKHvtyn4SubjLSo6vaiLFG2xm645NxE30wcR0QXwXpETKIGgdiBwZ4q8eVTzfTHEt4FORMNnsRk+hJvgNct0W+1FTaG8q4l0UWWxU5w5tUrHhBEw0qfITIOROgm3FA3o155rS5LDM0di7RvIH7U7Y5P7wg68099P+T6oezGZAe0AAAAASUVORK5CYII=");

        public static byte[] Row2ImageBinary => Convert.FromBase64String(
            "iVBORw0KGgoAAAANSUhEUgAAABAAAAAQCAYAAAAf8/9hAAAABGdBTUEAAK/INwWK6QAAABl0RVh0U29mdHdhcmUAQWRvYmUgSW1hZ2VSZWFkeXHJZTwAAAKqSURBVDjLfVNdSBRRFP7mZ2dz3dwfx3VZTVuRiizMwBAiJCixh34gEwLfiqCXegl66D2kB1mJygcjevO1FGKFUhMKI5fZNFYlV31wN7F2nZVk52dnuveuKxjaHc49d2bu953v3HMuZ9s2/jdisdgr0zR7DMMQiQfxzHRdp36I248gHo97CGDQ6/V2OZ0uaAQEywLbTTA+nxfR6DuIe4EVRemkYFmWa8rLy7G0vAoa3bJsWLZFiGwUCJmmaRATiYRWKBQkYmxTSSb12Wz2X8nIZLKEyMLFjkvsm0jBtbV12NhQWXSbPjZVaTOjLyQm/D4Pht++QdOJFhad/mMKKCsFPxyMkW0cOI7MxYl5apu5HJ7caYWPn0coNQrBfRJ/Pk9AthrATpZGpRt9/krwPA+OGL9tdE0JxM33aKsDyuQeeMIt2Fg6CjX6uqiAnAkh4CEIwi6Ckj8dmIXf5YA7eBaZxQQkTsfBiiA8laEiAS0Px3MQRHE3mKg6XKbgZhsHb/gCtPQQJBeHFWUOhilgJOmGSE+SHghNgScKwgEXy5+OKnxFZ2MCnsYryK8OgJdMONz1EPNJ5I/dQ3Z2tEjAb+dJbXGh2AuNkoLu6yDgawT8ArzDhJ4LY+3jNJKV3Qi4a4plpJPDIaL31ilWHsMwkVsZQ3WZhUDTZeg/ByFINvJqPdITX/AjeBuGUMFU0/RFUsu+8fEP7aVGsdem6tvPhGTDrkF65il8AQn5zCGkJmOYdV3FkYbjTGGwuoo126670N/ffz+kDUdu3H2J+aEHSC1/grOuGVtrqjG11ez4pTl3upQaacK5nbsQiUSqSBkjC8l16N9HEG4+B3X9N5LTCdsvV7Q+6n0e3+ve8KUFSSGjqur0t7kU+gaeYXIsihXODeXA+ZmOx/H4ftf9L53Qf7mz5LNnAAAAAElFTkSuQmCC");

        public static byte[] Row3ImageBinary => Convert.FromBase64String(
            "iVBORw0KGgoAAAANSUhEUgAAABAAAAAQCAYAAAAf8/9hAAAABGdBTUEAAK/INwWK6QAAABl0RVh0U29mdHdhcmUAQWRvYmUgSW1hZ2VSZWFkeXHJZTwAAAHYSURBVDjLlVLPS1RxHJynpVu7KEn0Vt+2l6IO5qGCIsIwCPwD6hTUaSk6REoUHeoQ0qVAMrp0COpY0SUIPVRgSl7ScCUTst6zIoqg0y7lvpnPt8MWKuuu29w+hxnmx8dzzmE5+l7mxk1u/a3Dd/ejDjSsII/m3vjJ9MF0yt93ZuTkdD0CnnMO/WOnmsxsJp3yd2zfvA3mHOa+zuHTjy/zojrvHX1YqunAZE9MlpUcZAaZQBNIZUg9XdPBP5wePuEO7eyGQXg29QL3jz3y1oqwbvkhCuYEOQMp/HeJohCbICMUVwr0DvZcOnK9u7GmQNmBQLJCgORxkneqRmAs0BFmDi0bW9E72PPda/BikwWi0OEHkNR14MrewsTAZF+lAAWZEH6LUCwUkUlntrS1tiG5IYlEc6LcjYjSYuncngtdhakbM5dXlhgTNEMYLqB9q49MKgsPjTBXntVgkDNIgmI1VY2Q7QzgJ9rx++ci3ofziBYiiELQEUAyhB/D29M3Zy+uIkDIhGYvgeKvIkbHxz6Tevzq6ut+ANh9fldetMn80OzZVVdgLFjBQ0tpEz68jcB4ifx3pQeictVXIEETnBPCKMLEwBIZAPJD767V/ETGwsjzYYiC6vzEP9asLo3SGuQvAAAAAElFTkSuQmCC");

        public static byte[] Row4ImageBinary => Convert.FromBase64String(
            "iVBORw0KGgoAAAANSUhEUgAAABAAAAAQCAYAAAAf8/9hAAAABGdBTUEAAK/INwWK6QAAABl0RVh0U29mdHdhcmUAQWRvYmUgSW1hZ2VSZWFkeXHJZTwAAAJFSURBVDjLpZNfaM0BFMc/v7s/pv25tnsXlrzo2nav3d3y4CaelqLw4l1JEVaUl1GeUHtQXpiSUvKqZFFWJtFajO62K/KwlXQn7syfe+3PPX883AkNKefl1KnzOed8zzmBu/M/Vvm74OnMiayZJlTNVfXO2fT5nX8ChJYm9zRhJFrrWok1xAJRTf+tgyWAU6neDwuyUCx5ieJCEREZ+xsgcHfOjJ50M0XV0LL39sa2QEwYnRr7JKKqqiER4cru641LNFBT1tfGMDfMHccCNcMd4s3xsLribjyeePp7EVUVdcPcyBVe83HuI+KCuRMKKjBz1oVjiMgfAKJk81kaqsKsrG3h/dc86loex+dRUwQlUhdhz7VdLiKIKLcPDATBz3dwbPCgx5vjZKczqBnmirihrjRUhVlTvxYzxzEGRx5w99Bg8MsdiCjqimjZ62KymmIz87x5+YLZ2SLF+QJuxR8jHL13wEWUFTUrUDNKXiprYoqYUZ13ossr2Lh1E2uaYtx/fpPh7EPS3S3nQt8rJ1a2syq8isnPE8SbkiSakiQiKTqiKWSqSKqtEw0pnau3oUGJdMdmgCOVACURBCXz7hkbop1MvJ0kl59CVYmGo8x8zlMV1LGjfT8Ax7su0z/eB9yqqQSQkqBmJCJJRI1cfoobe/sDgO2XurxQmOZ5bojR3CN6tl2ld2AfNRXLAObKABGevBpBVFlc0dwPYcWorw2Gx4aCzckt9I/3UR1U8ijzAOBi8K/vnO5u6QUOA/XAF6Bv+EKu5xvVXGolRpHH+AAAAABJRU5ErkJggg==");

        public static SqliteColumn IdColumn => new SqliteColumn("id", StoragesClasses.StorageClassType.IntegerClass, true);
        public static SqliteColumn FirstNameColumn => new SqliteColumn("first_name", StoragesClasses.StorageClassType.TextClass, true);
        public static SqliteColumn LastNameColumn => new SqliteColumn("last_name", StoragesClasses.StorageClassType.TextClass, true);
        public static SqliteColumn EmailColumn => new SqliteColumn("email", StoragesClasses.StorageClassType.TextClass, false);
        public static SqliteColumn GenderColumn => new SqliteColumn("gender", StoragesClasses.StorageClassType.TextClass, true);
        public static SqliteColumn ImageColumn => new SqliteColumn("image", StoragesClasses.StorageClassType.BlobClass, true);
        public static SqliteColumn SalaryColumnReal => new SqliteColumn("salary", StoragesClasses.StorageClassType.RealClass, false);
        public static SqliteColumn SalaryColumnNumeric => new SqliteColumn("salary", StoragesClasses.StorageClassType.NumericClass, false);


        public static int ExpectedTableRows => 1000;
        public static int ExpectedQueryRows => 3;

        public static string[] IdIndex = { "id" };
        public static string[] ExpectedTableIndex = { "last_name", "gender" };
        public static string[] ExpectedQueryIndex = { "email" };

        public static object[] TableRow1 => new object[]
            { 1, "Shoshana", "Weinham", "sweinham0@cafepress.com", "Female", Row1ImageBinary, 19000.28M };

        public static object[] TableRow2 => new object[]
            { 2, "Coraline", "Hundal", "chundal1@webeden.co.uk", "Female", Row2ImageBinary, 38185.39M };

        public static object[] TableRow3 => new object[]
            { 3, "Calv", "Murton", DBNull.Value, "Non-binary", Row3ImageBinary, 22776.57M };

        public static object[] TableRow4 => new object[]
            { 4, "Toinette", "Reedshaw", DBNull.Value, "Female", Row4ImageBinary, 28294.83M };

        public static object[] QueryRow1 => new object[]
            { 1, "Shoshana", "Weinham", "sweinham0@cafepress.com" };

        public static object[] QueryRow2 => new object[]
            { 2, "Coraline", "Hundal", "chundal1@webeden.co.uk" };

        public static object[] QueryRow3 => new object[]
            { 3, "Calv", "Murton", DBNull.Value };

        public static AndConstraint<SqliteTableAssertions> ShouldPassTableDataConditions(this SqliteTable? table)
        {
            return table
                .Should()
                .Exist().And
                .HaveColumn(IdColumn).And
                .HaveColumn(FirstNameColumn).And
                .HaveColumn(LastNameColumn).And
                .HaveColumn(EmailColumn).And
                .HaveColumn(GenderColumn).And
                .HaveColumn(ImageColumn).And
                .HaveColumn(SalaryColumnReal, SalaryColumnNumeric).And
                .HaveColumnCount(7).And
                .HaveIndex(IdIndex).And
                .HaveIndex(ExpectedTableIndex).And
                .HaveRowCount(ExpectedTableRows).And
                .HaveRowMatching(TableRow1).And
                .HaveRowMatching(TableRow2).And
                .HaveRowMatching(TableRow3).And
                .HaveRowMatching(TableRow4);
        }

        /// <summary>
        /// Assumes <see cref="QuerySql"/> and <see cref="ExpectedQueryIndex" />.
        /// </summary>
        /// <param name="table"></param>
        /// <returns></returns>
        public static AndConstraint<SqliteTableAssertions> ShouldPassQueryDataConditions(this SqliteTable? table)
        {
            return table
                .Should()
                .Exist().And
                .HaveColumn(IdColumn).And
                .HaveColumn(FirstNameColumn).And
                .HaveColumn(LastNameColumn).And
                .HaveColumn(EmailColumn).And
                .HaveColumnCount(4).And
                .HaveIndex(ExpectedQueryIndex).And
                .HaveRowCount(ExpectedQueryRows).And
                .HaveRowMatching(QueryRow1).And
                .HaveRowMatching(QueryRow2).And
                .HaveRowMatching(QueryRow3);
        }

        public static AndConstraint<SqliteTableAssertions> ShouldPassFooConditions(this SqliteTable? table)
        {
            return table
                .Should()
                .Exist().And
                .HaveColumnCount(2).And
                .HaveIndex(IdIndex).And
                .HaveRowCount(1);
        }
    }
}
