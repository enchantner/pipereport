import os
import shutil
import tempfile
from typing import Optional

from git import Git, Repo

from pipereport.template.registry.fs import FSTemplateRegistry


class GitFSTemplateRegistry(FSTemplateRegistry):
    def __init__(self, *args, repo: Optional[str] = None, **kwargs):
        if repo is not None:
            self.repo = repo
        else:
            self.repo = self.check_env(
                "GIT_TEMPLATE_REGISTRY_URL",
                exc_text="cannot clone template registry repo"
            )

        self.identity = self.check_env(
            "GET_TEMPLATE_IDENTITY_FILE",
            default=os.path.expanduser("~/.ssh/id_rsa")
        )

        self.tmpdir = tempfile.mkdtemp()
        path = os.path.join(str(self.tmpdir), 'templates')
        
        git_ssh_cmd = f"ssh -i '{self.identity}'"
        with Git().custom_environment(GIT_SSH_COMMAND=git_ssh_cmd):
            Repo.clone_from(self.repo, path)
        super().__init__(path=path, *args, **kwargs)

    def __del__(self):
        shutil.rmtree(self.tmpdir)
    
