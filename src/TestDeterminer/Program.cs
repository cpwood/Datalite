using System.Diagnostics;
using System.Reflection;
using System.Text;
using Newtonsoft.Json.Linq;

/*
 * Discovers which projects have changed as part of the last commit and then
 * uses the tests.json file to determine which test projects should be executed.
 * This is done to try and keep the Action as streamlined as possible given it can
 * take several minutes to start a Docker image for SQL Server, for example. We
 * should avoid doing this if there are no SQL Server changes.
 */

var dll = new FileInfo(Assembly.GetExecutingAssembly().Location);
var dir = dll.Directory!;

var jsonFile = Path.Combine(dir.FullName, "tests.json");

while (dir.Name != "src")
{
    dir = dir.Parent!;
}

var directory = dir.Parent!.FullName;

Console.WriteLine($"Working directory is '{directory}'..");

var psi = new ProcessStartInfo
{
    FileName = "git",
    Arguments = "diff --name-only HEAD~",
    WorkingDirectory = directory,
    UseShellExecute = false,
    RedirectStandardOutput = true
};

Console.WriteLine("Getting changes..");

var process = Process.Start(psi);
var output = new StringBuilder();

while (!process!.StandardOutput.EndOfStream)
{
    var line = await process.StandardError.ReadLineAsync();
    output.AppendLine(line);
}

process.WaitForExit();

var changes = output.ToString().Trim();

Console.WriteLine("Discovering tests..");

using var reader = new StringReader(changes);
var change = await reader.ReadLineAsync();

var filters = new List<string>();
var mappings = JArray.Parse(await File.ReadAllTextAsync(jsonFile));

while (!string.IsNullOrEmpty(change))
{
    foreach (var mapping in mappings)
    {
        if (change.StartsWith(mapping["path"]!.Value<string>()!))
            filters.AddRange(mapping["filters"]!.Values<string>()!);
    }

    change = await reader.ReadLineAsync();
}

Console.WriteLine("Writing filter.txt ..");

var filter = string.Join('|', filters.OrderBy(x => x).Distinct().ToArray());
await File.WriteAllTextAsync(Path.Combine(directory, "filter.txt"), filter);

Console.WriteLine($"Filter is: '{filter}'");
Console.WriteLine("Done!");