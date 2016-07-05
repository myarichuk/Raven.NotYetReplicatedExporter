using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Util;
using Raven.Client.Document;
using Raven.Json.Linq;
using ServiceStack.Text;

namespace Raven.NotYetReplicatedExporter
{	
	public class DocumentInfoRow
	{		
		public string Id { get; set; }

		public string Etag { get; set; }

		public string EntityName { get; set; }

		public string DestinationUrl { get; set; }
	}

	class Program
	{
		static void Main(string[] args)
		{
			if (args.Length != 2)
			{
				Console.WriteLine("Exporter of not-yet-exported documents in RavenDB");
				Console.WriteLine("Usage : Raven.NotYetReplicatedExporter.exe [database url] [database name]");
				return;
			}

			if (String.IsNullOrWhiteSpace(args[0]))
			{
				Console.WriteLine("Empty url entered...needs to have a value.");
				return;
			}

			if (String.IsNullOrWhiteSpace(args[1]))
			{
				Console.WriteLine("Empty database name entered...needs to have a value.");
				return;
			}

			try
			{
				new Uri(args[0]);
			}
			catch (UriFormatException e)
			{
				Console.WriteLine("Failed to parse database url. Reason: " + e);
				return;
			}

			try
			{
				using (var store = new DocumentStore
				{
					DefaultDatabase = args[1],
					Url = args[0]
				})
				{
					store.Initialize();
					var unreplicatedDocs = new List<DocumentInfoRow>();

					var lastDocEtag = store.DatabaseCommands.GetStatistics().LastDocEtag;
					var replicationStats = store.DatabaseCommands.Info.GetReplicationInfo();
					using (var sw = new StreamWriter($"Not-yet-replicated-at-{store.DefaultDatabase}.csv", false))
					{
						foreach (var destStats in replicationStats.Stats)
						{
							if (EtagUtil.IsGreaterThan(destStats.LastReplicatedEtag, lastDocEtag))
								continue;
							using (var session = store.OpenSession())
							using (var unReplicatedDocsStream = session.Advanced.Stream<dynamic>(destStats.LastReplicatedEtag))
							{
								while (unReplicatedDocsStream.MoveNext())
								{
									if (unReplicatedDocsStream.Current.Key.StartsWith("Raven/") ||
									    unReplicatedDocsStream.Current == null) //precaution
										continue;

									var entityName = string.Empty;
									RavenJToken val;
									if (unReplicatedDocsStream.Current.Metadata.TryGetValue(Constants.RavenEntityName, out val))
										entityName = val.Value<string>();									
									unreplicatedDocs.Add(new DocumentInfoRow
									{
										Id = unReplicatedDocsStream.Current.Key,
										Etag = unReplicatedDocsStream.Current.Etag.ToString(),
										EntityName = entityName,
										DestinationUrl = destStats.Url
									});
									Console.WriteLine($"Doc Id = {unReplicatedDocsStream.Current.Key}, Destination = {destStats.Url}");
								}
							}
						}

						CsvWriter<DocumentInfoRow>.Write(sw, unreplicatedDocs);
						sw.Flush();
					}
				}
			}
			catch (Exception e)
			{
				Console.WriteLine($"Failed to export not-yet-replicated documents. Reason: {e}");
			}
		}

	}
}
