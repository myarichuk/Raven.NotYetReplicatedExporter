using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Replication;
using Raven.Abstractions.Util;
using Raven.Client.Document;
using Raven.Json.Linq;
using ServiceStack;
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

					var lastDocEtag = store.DatabaseCommands.GetStatistics().LastDocEtag;
					int count = 0;
					ReplicationDocument destinationsDoc;
					using (var session = store.OpenSession())
					{
						destinationsDoc = session.Load<ReplicationDocument>("Raven/Replication/Destinations");						
					}
					
					using (var sw = new StreamWriter($"Not-yet-replicated-at-{store.DefaultDatabase}.csv", false))
					{
						foreach (var dest in destinationsDoc.Destinations)
						{
							var replicationInformation = GetReplicationInformation($"{dest.Url}/databases/{dest.Database}", args[0], destinationsDoc.Source);
							var lastReplicatedEtag = Etag.Parse(replicationInformation.LastDocumentEtag);
							if (EtagUtil.IsGreaterThan(lastReplicatedEtag, lastDocEtag))
								continue;
							using (var session = store.OpenSession())
							using (var unReplicatedDocsStream = session.Advanced.Stream<dynamic>(lastReplicatedEtag))
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

									Console.WriteLine($"Doc Id = {unReplicatedDocsStream.Current.Key}, Destination = {dest.Url}");
									CsvWriter<DocumentInfoRow>.WriteObjectRow(sw, new DocumentInfoRow
									{
										Id = unReplicatedDocsStream.Current.Key,
										Etag = unReplicatedDocsStream.Current.Etag.ToString(),
										EntityName = entityName,
										DestinationUrl = dest.Url
									});
									CsvConfig<DocumentInfoRow>.OmitHeaders = true;
									if (count++ % 1000 == 0)
										sw.Flush();
								}
							}
						}
						sw.Flush();
					}
				}
			}
			catch (Exception e)
			{
				Console.WriteLine($"Failed to export not-yet-replicated documents. Reason: {e}");
			}
		}

		private static SourceReplicationInformationWithBatchInformation GetReplicationInformation(string destinationUrl, string sourceUrl, string sourceDbId)
		{
			var client = new JsonHttpClient(destinationUrl);
			var result =
				client.Get<SourceReplicationInformationWithBatchInformation>($"/replication/lastEtag?from={sourceUrl}&dbid={sourceDbId}");
			return result;
		}
	}

	public class SourceReplicationInformation
	{
		public string LastDocumentEtag { get; set; }

		[Obsolete("Use RavenFS instead.")]
		public string LastAttachmentEtag { get; set; }

		public Guid ServerInstanceId { get; set; }

		public string Source { get; set; }

		public DateTime? LastModified { get; set; }

		public int? LastBatchSize { get; set; }

		public override string ToString()
		{
			return string.Format("LastDocumentEtag: {0}, LastAttachmentEtag: {1}", LastDocumentEtag, LastAttachmentEtag);
		}

		public SourceReplicationInformation()
		{
			LastDocumentEtag = Etag.Empty.ToString();
			LastAttachmentEtag = Etag.Empty.ToString();
		}
	}

	public class SourceReplicationInformationWithBatchInformation : SourceReplicationInformation
	{
		public int? MaxNumberOfItemsToReceiveInSingleBatch { get; set; }
	}

}
