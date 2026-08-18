"""Crash-recoverable bridge between the run ledger and Git worktrees."""

from __future__ import annotations

from collections.abc import Callable
from typing import Literal

from forge_replay.persistence import RunWorkspaceRecord
from forge_replay.ports import WorkspaceStorePort
from forge_replay.workspace.git_worktree import GitWorktreeManager

WorkspaceHook = Callable[[str], None]


class WorkspaceController:
    def __init__(
        self,
        store: WorkspaceStorePort,
        manager: GitWorktreeManager,
        *,
        process_instance_id: str,
        hook: WorkspaceHook | None = None,
    ):
        self.store = store
        self.manager = manager
        self.process_instance_id = process_instance_id
        self.hook = hook

    def provision(
        self,
        run_id: str,
        *,
        dirty_mode: Literal["refuse", "head-only"] = "refuse",
    ) -> RunWorkspaceRecord:
        current = self.store.get_run_workspace(run_id)
        if current.worktree_path is not None:
            return current
        self.store.begin_workspace_provisioning(
            run_id=run_id,
            dirty_mode=dirty_mode,
            process_instance_id=self.process_instance_id,
        )
        owned = self.manager.load_owned(run_id)
        if owned is None:
            owned = self.manager.provision(
                run_id=run_id,
                repo_root=current.base_repo_root,
                dirty_mode=dirty_mode,
            )
        if owned.base_commit_sha != current.base_commit_sha:
            raise RuntimeError("owned worktree marker has the wrong base commit")
        if self.hook is not None:
            self.hook("after_worktree_created_before_ledger_attach")
        return self.store.attach_provisioned_workspace(
            run_id=run_id,
            worktree_path=owned.worktree_path,
            branch=owned.branch,
            base_commit_sha=owned.base_commit_sha,
            ownership_marker=owned.ownership_marker,
            ownership_token=owned.ownership_token,
            process_instance_id=self.process_instance_id,
        )
