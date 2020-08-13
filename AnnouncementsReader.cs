using System;
using System.Collections.Generic;
using System.Linq;
using Papirus.BackEnd.DataAccessObjects;
using Papirus.BackEnd.DbAccess;
using Papirus.BackEnd.Drafts;
using Papirus.BackEnd.Elasticsearch.Crawler;
using Papirus.BackEnd.TaskRead;
using Papirus.BackEnd.TaskRequests;
using Papirus.BackEnd.TaskWithNotes;
using Papirus.Measuring;
using Pyrus.Backend.Data.Contracts.Draft;
using Pyrus.Backend.Data.Contracts.Task;
using Pyrus.Backend.Data.DataObjects.TaskNoteFiles;
using Pyrus.Backend.Data.Storer;

namespace Papirus.BackEnd.TaskLists
{
	internal class AnnouncementReader : TaskListReader
	{
		public AnnouncementReader(TaskListParams taskListParams, int loginId, HashSet<int> roles, Measurer measurer)
			: base(taskListParams, loginId, roles, measurer)
		{
		}

		public override List<ITask> ProcessImpl(IDataConnection connection)
		{
			if (Params.Type != PageMode.Blog)
				throw new ArgumentException(nameof(Params));

			var result = new List<ITask>();
			var tasks = GetAnnouncements(connection, Params.ActivityState == TaskActivityState.Active);

			if (tasks.Count == 0)
				return result;

			var taskIds = tasks.Select(x => x.TaskId).ToList();

			var notes = Params.IncludeExtendedInfo
				? TaskNoteDbHelper.GetTaskNoteByTaskIds(taskIds, connection).ToLookup(x => x.TaskId, x => x)
				: default;

			var attachments = Params.IncludeExtendedInfo
				? StorerHelper.GetTaskAttachmentStorer(connection).GetAttachmentsByTaskIds(taskIds).ToLookup(x => x.TaskId, x => x)
				: default;

			var drafts = Params.IncludeExtendedInfo
				? DraftDAO.Instance.GetDraftsByTaskIds(taskIds, false, connection, LoginId).ToLookup(x => x.TaskId, x => x)
				: default;

			TaskDAO.Instance.EnsureEntitiesInCache(taskIds, connection, Measurer);
			foreach (var task in tasks)
			{
				var commonHeader = TaskDAO.Instance.GetItem(task.TaskId);

				var taskListEntry = new CachedTaskListEntry
				{
					CommonHeader = commonHeader,
					PersonalHeader = new CachedPersonalTaskHeader(
						task.TaskId,
						LoginId,
						task.LastNoteId,
						task.LastReadNoteId,
						task.LastVisibleNoteId,
						task.Category,
						task.PlanDate,
						followed: TaskFollowingDAO.Instance.GetItemFast(task.TaskId)?.Contains(LoginId) == true
					),
				};

				if (notes != default(ILookup<int, TaskNote>))
					taskListEntry.Notes = notes[task.TaskId].ToList();

				if (attachments != default(ILookup<int, TaskNoteFileBase>))
				{
					var notesAttachments = attachments[task.TaskId].ToLookup(x => x.NoteId, x => x);

					taskListEntry.Attachments = notesAttachments[null].ToList();
					if (taskListEntry.Notes != null)
					{
						foreach (var note in taskListEntry.Notes)
						{
							note.Attachments = notesAttachments[note.Id].ToList();
						}
					}
				}

				if (drafts != default(ILookup<int?, Draft>))
				{
					var taskDrafts = drafts[task.TaskId];
					if (taskDrafts.Any())
						taskListEntry.Draft = taskDrafts.Aggregate((seed, x) => x.Date > seed.Date ? x : seed);
				}

				result.Add(taskListEntry);
			}

			return result;
		}

		private List<TaskInfo> GetAnnouncements(IDataConnection connection, bool onlyActive)
		{
			return StorerHelper
				.GetTaskInfoStorer(connection)
				.GetAnnouncements(LoginId, onlyActive, ItemsCountForHasNextCheck);
		}
	}
}
