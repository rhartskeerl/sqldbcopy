using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SqlDbCopy
{
    public static class Extensions
    {
        public static string ToCsv(this SqlDataReader r, string delimeter = ",")
        {
            StringBuilder sb = new StringBuilder();

            while (r.Read())
            {
                for (int i = 0; i < r.FieldCount; i++)
                {
                    if (!r.IsDBNull(i))
                    {
                        switch (r.GetDataTypeName(i))
                        {
                            case "varbinary":
                                sb.Append(BitConverter.ToString((byte[])r.GetValue(i)).Replace("-", ""));
                                break;
                            case "datetime":
                            case "datetime2":
                                sb.Append(r.GetDateTime(i).ToString("O"));
                                break;
                            case "int":
                            case "bigint":
                            case "tinyint":
                            case "smallint":
                            case "decimal":
                            case "numeric":
                            case "bit":
                                sb.Append(r.GetValue(i).ToString());
                                break;
                            default:
                                sb.Append("\"" + r.GetValue(i).ToString() + "\"");
                                break;
                        }
                    }

                    if (i < r.FieldCount - 1)
                        sb.Append(delimeter);
                }
                sb.Append("\r\n");
            }

            return sb.ToString();
        }

        public static string ToQuotedName(this string name)
        {
            string[] parts = name.Split('.');
            for (int i = 0; i < parts.Length; i++)
            {
                if(!string.IsNullOrEmpty(parts[i]))
                    parts[i] = "[" + parts[i] + "]";
            }
            return String.Join(".", parts);
        }

    }
}
