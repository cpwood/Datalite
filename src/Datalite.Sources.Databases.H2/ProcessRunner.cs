using System.Diagnostics;
using System.Text;
using System.Threading.Tasks;
using Datalite.Exceptions;

namespace Datalite.Sources.Databases.H2
{
    internal class ProcessRunner : IProcessRunner
    {
        public async Task<string> RunAsync(ProcessStartInfo psi)
        {
            psi.UseShellExecute = false;
            psi.RedirectStandardError = true;

            var process = Process.Start(psi);

            if (process == null)
                throw new DataliteException("Java bridge process didn't start.");

            var output = new StringBuilder();

            while (!process.StandardError.EndOfStream)
            {
                var line = await process.StandardError.ReadLineAsync();
                output.AppendLine(line);
            }

            process.WaitForExit();

            return output.ToString();
        }
    }
}
