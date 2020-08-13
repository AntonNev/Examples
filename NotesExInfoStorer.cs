using System;
using System.Collections.Generic;
using System.Linq;
using LinqToDB;
using LinqToDB.Tools;
using Papirus.BackEnd.DbAccess;
using Pyrus.Backend.Data.Contracts;
using Pyrus.Backend.Data.Contracts.Persons;
using Pyrus.Backend.Data.Contracts.Task;
using Pyrus.Backend.Data.DataObjects;
using Pyrus.Backend.Data.Storer.Contract;

namespace Pyrus.Backend.Data.Storer
{
	public abstract class NotesExInfoStorer : StorerBase, INotesExInfoStorer
	{
		protected NotesExInfoStorer(IDataConnection connection) : base(connection)
		{
		}

		protected NotesExInfoStorer(IDataContext context) : base(context)
		{
		}

		public virtual IEnumerable<PersonIdWithNoteId> GetAnnouncementsFromDb(int lastSentNoteId)
		{
			return DataContext.Webpm.UserAnnouncements
				.InnerJoin(
					DataContext.Webpm.PersonProfiles,
					(ua, pp) => ua.PersonId == pp.PersonId,
					(ua, pp) => new { UserAnnouncement = ua, PersonProfile = pp })
				.InnerJoin(
					DataContext.Webpm.NotesExInfo,
					(x, n) => x.UserAnnouncement.TaskId == n.TaskId,
					(x, n) => new { x.UserAnnouncement, x.PersonProfile, Note = n })
				.Where(x => x.Note.Id > lastSentNoteId
							&& x.UserAnnouncement.LastPersonReadNoteId == 0
							&& (x.PersonProfile.LastMailDateTimeUtc == null
								|| x.PersonProfile.LastMailDateTimeUtc < Sql.DateAdd(Sql.DateParts.Minute, -15, Sql.CurrentTimestampUtc))
							&& x.PersonProfile.LastSentNoteId < x.Note.Id
							&& (x.PersonProfile.Flags & PersonFlags.CanSendMailByBatches) != 0)
				.Select(x => new { x.PersonProfile.PersonId, x.Note.Id })
				.Distinct()
				.AsEnumerable()
				.Select(x => new PersonIdWithNoteId(x.PersonId, x.Id, true));
		}

		public IEnumerable<int> GetChangedTaskIds(int formId, DateTime startDateUtc, DateTime endDateUtc)
		{
			return DataContext.Webpm.NotesExInfo
				.InnerJoin(
					DataContext.Webpm.Tasks,
					(n, t) => n.TaskId == t.Id,
					(n, t) => new { Note = n, Task = t })
				.Where(x => x.Task.TemplateId == formId && x.Note.CreationDateTimeUtc.Between(startDateUtc, endDateUtc))
				.OrderBy(x => x.Task.Id)
				.Select(x => x.Task.Id)
				.Distinct();
		}

		public Tuple<int, DateTime?> GetNewCommentsCount(int taskId, long startNoteId, int userId)
		{
			var result = DataContext.Webpm.NotesExInfo
					.LeftJoin(
						DataContext.Webpm.UserTasks,
						(n, ut) => n.TaskId == ut.TaskId && n.Id == ut.LastPersonCommentNoteId,
						(n, ut) => new { Note = n, PersonId = (int?)ut.PersonId })
					.Where(x => x.Note.TaskId == taskId && x.Note.Id > startNoteId && (x.PersonId == null || x.PersonId == userId))
					.GroupBy(x => x.PersonId ?? -1)
					.Select(x => new { Count = x.Count(), Date = x.Max(y => y.Note.CreationDateTimeUtc) })
					.FirstOrDefault();

			return result == null
				? null
				: result.Count > 0
					? Tuple.Create(result.Count, result.Date)
					: Tuple.Create(result.Count, new DateTime?());
		}

		public virtual IEnumerable<PersonIdWithNoteId> GetNotesFromDb(int lastSentNoteId)
		{
			return DataContext.Webpm.NotesExInfo
				.InnerJoin(
					DataContext.Webpm.UserTasks,
					(n, ut) => n.TaskId == ut.TaskId,
					(n, ut) => new { Note = n, UserTask = ut })
				.InnerJoin(
					DataContext.Webpm.PersonProfiles,
					(x, pp) => x.UserTask.PersonId == pp.PersonId,
					(x, pp) => new { x.Note, x.UserTask, PersonProfile = pp })
				.Where(x => x.Note.Id > lastSentNoteId
							&& x.UserTask.FolderId == TaskCategory.Inbox
							&& x.UserTask.LastPersonReadNoteId < x.Note.Id
							&& x.PersonProfile.LastSentNoteId < x.Note.Id
							&& (x.PersonProfile.LastMailDateTimeUtc == null
								|| x.PersonProfile.LastMailDateTimeUtc < Sql.DateAdd(Sql.DateParts.Minute, -15, Sql.CurrentTimestampUtc))
							&& (x.PersonProfile.Flags & PersonFlags.CanSendMailByBatches) != 0)
				.Select(x => new { x.PersonProfile.PersonId, x.Note.Id })
				.Distinct()
				.AsEnumerable()
				.Select(x => new PersonIdWithNoteId(x.PersonId, x.Id, false));
		}

		public abstract bool Insert(NotesExInfoDataObject notesExInfo, Guid? clientId);

		public virtual NotesExInfoDataObject GetItem(long key)
		{
			return DataContext.Webpm.NotesExInfo.FirstOrDefault(x => x.Id == key);
		}

		public virtual IEnumerable<NotesExInfoDataObject> GetItems(IEnumerable<long> keys)
		{
			return DataContext.Webpm.NotesExInfo.Where(x => keys.Contains(x.Id));
		}

		public virtual long GetLastSentNoteId(DateTime sinceDateTime)
		{
			return DataContext.Webpm.NotesExInfo
				.Where(x => x.CreationDateTimeUtc < sinceDateTime)
				.Select(x => x.Id)
				.OrderByDescending(x => x)
				.FirstOrDefault();
		}

		public virtual Tuple<int, int> GetTaskIdWithNoteAuthor(long id)
		{
			return DataContext.Webpm.NotesExInfo
				.Where(x => x.Id == id)
				.Select(x => Tuple.Create((int)x.TaskId, (int)x.AuthorId))
				.FirstOrDefault();
		}

		public virtual IEnumerable<NotesExInfoDataObject> GetTaskHistory(int taskIdMin, int taskIdMax)
		{
			return DataContext.Webpm.NotesExInfo
				.Where(x => x.TaskId.Between(taskIdMin, taskIdMax))
				.OrderBy(x => x.TaskId)
				.ThenBy(x => x.Id);
		}

		public virtual IEnumerable<NotesExInfoDataObject> GetTaskHistory(ICollection<int> taskIds)
		{
			if (taskIds == null)
			{
				throw new ArgumentNullException(nameof(taskIds));
			}

			if (taskIds.Count == 0)
			{
				return new NotesExInfoDataObject[0];
			}

			var taskIdsLong = taskIds.Select(x => (long)x);

			return DataContext.Webpm.NotesExInfo
				.Where(x => taskIdsLong.Contains(x.TaskId))
				.OrderBy(x => x.TaskId)
				.ThenBy(x => x.Id);
		}

		public virtual NotesExInfoDataObject GetTaskNote(long noteId)
		{
			return DataContext.Webpm.NotesExInfo
				.Where(x => x.Id == noteId)
				.Select(x => new NotesExInfoDataObject(x.Id, x.TaskId, x.AuthorId, x.Version, x.ConstantDescription,
					x.PrivateDescription, x.FilterableDescription, "", x.CreationDateTimeUtc))
				.FirstOrDefault();
		}

		public virtual bool HasDuplicates(int personId, Guid clientId, out int taskId, out long noteId)
		{
			var res = DataContext.Webpm.NotesExInfo
				.InnerJoin(DataContext.Webpm.NoteClientIds, (nei, nci) => nei.Id == nci.Id, (nei, nci) => new { nei.Id, nei.TaskId, nci.ClientId, nci.PersonId })
				.Where(x => x.PersonId == personId && x.ClientId == clientId)
				.Select(x => new { x.TaskId, x.Id })
				.FirstOrDefault();

			if (res == null)
			{
				taskId = 0;
				noteId = 0;
				return false;
			}

			taskId = (int)res.TaskId;
			noteId = res.Id;
			return true;
		}

		public virtual IEnumerable<(int taskId, int noteId)> GetDuplicates(IEnumerable<Guid> clientIds)
		{
			return DataContext.Webpm.NoteClientIds
				.InnerJoin(DataContext.Webpm.NotesExInfo, (nci, nei) => nci.Id == nei.Id,
					(nci, nei) => new { nei.TaskId, nei.Id, nci.ClientId })
				.Where(x => clientIds.Contains(x.ClientId))
				.Select(x => ValueTuple.Create((int)x.TaskId, (int)x.Id));
		}

		public virtual int[] ReadTaskIdsByLastModification(DateTime sinceUtc, DateTime untilUtc)
		{
			return DataContext.Webpm.NotesExInfo
				.Where(x => x.CreationDateTimeUtc.Between(sinceUtc, untilUtc))
				.Select(x => x.TaskId)
				.Distinct()
				.OrderBy(x => x)
				.AsEnumerable()
				.Select(x => (int)x)
				.ToArray();
		}

		public virtual int GetNotesCount(int taskId)
		{
			return DataContext.Webpm.NotesExInfo.Count(x => x.TaskId == taskId);
		}

		public Dictionary<int, List<NotesExInfoDataObject>> GetNotesForCommunicationsCrawler(IEnumerable<long> taskIds)
		{
			return DataContext.Webpm.NotesExInfo
				.Where(n => n.TaskId.In(taskIds) && n.AuthorId != 1730 && n.CreationDateTimeUtc != null)
				.ToList()
				.GroupBy(n => n.TaskId)
				.ToDictionary(n => (int)n.Key, n => n.ToList());

		}
	}
}
