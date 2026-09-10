import unittest

from scaler.worker_manager.nested.orb_aws_ec2.worker_manager import ORBAWSEC2WorkerManager, _extract_git_url_and_branch


class TestORBAWSEC2WorkerManagerValidateRequirements(unittest.TestCase):
    def test_raises_when_opengris_scaler_missing(self):
        requirements = "boto3\nrequests>=2.0\n"
        with self.assertRaises(ValueError):
            ORBAWSEC2WorkerManager._validate_requirements(requirements)

    def test_passes_when_opengris_scaler_present(self):
        requirements = "boto3\nopengris-scaler>=1.0\n"
        ORBAWSEC2WorkerManager._validate_requirements(requirements)

    def test_passes_with_underscore_variant(self):
        requirements = "opengris_scaler\n"
        ORBAWSEC2WorkerManager._validate_requirements(requirements)

    def test_passes_with_extras(self):
        requirements = "opengris-scaler[orb]\n"
        ORBAWSEC2WorkerManager._validate_requirements(requirements)

    def test_ignores_comments_and_flags(self):
        requirements = "# opengris-scaler\n-r base.txt\nboto3\n"
        with self.assertRaises(ValueError):
            ORBAWSEC2WorkerManager._validate_requirements(requirements)

    def test_passes_with_direct_url(self):
        requirements = "opengris-scaler\nhttps://example.com/mypackage.whl\n"
        ORBAWSEC2WorkerManager._validate_requirements(requirements)

    def test_raises_on_malformed_line(self):
        requirements = "opengris-scaler\n!!!invalid package!!!\n"
        with self.assertRaises(ValueError, msg="Invalid requirement line"):
            ORBAWSEC2WorkerManager._validate_requirements(requirements)

    def test_raises_on_local_path(self):
        requirements = "opengris-scaler\n./local_package\n"
        with self.assertRaises(ValueError, msg="Invalid requirement line"):
            ORBAWSEC2WorkerManager._validate_requirements(requirements)


class TestExtractGitURLAndBranch(unittest.TestCase):
    def test_https_with_branch(self):
        reqs = "scaler @ git+https://github.com/org/repo.git@main\n"
        self.assertEqual(_extract_git_url_and_branch(reqs), ("https://github.com/org/repo.git", "main"))

    def test_https_without_branch(self):
        reqs = "scaler @ git+https://github.com/org/repo.git\n"
        self.assertEqual(_extract_git_url_and_branch(reqs), ("https://github.com/org/repo.git", ""))

    def test_ssh_with_user_at_host_and_branch(self):
        reqs = "scaler @ git+ssh://git@github.com/org/repo.git@main\n"
        self.assertEqual(_extract_git_url_and_branch(reqs), ("ssh://git@github.com/org/repo.git", "main"))

    def test_https_with_token_auth(self):
        reqs = "scaler @ git+https://TOKEN@github.com/org/repo.git@main\n"
        self.assertEqual(_extract_git_url_and_branch(reqs), ("https://TOKEN@github.com/org/repo.git", "main"))

    def test_env_marker_stripped(self):
        reqs = 'scaler @ git+https://github.com/org/repo.git@main; python_version<"3.9"\n'
        self.assertEqual(_extract_git_url_and_branch(reqs), ("https://github.com/org/repo.git", "main"))

    def test_comment_line_ignored(self):
        reqs = "# scaler @ git+https://github.com/org/repo.git@main\nscaler\n"
        self.assertIsNone(_extract_git_url_and_branch(reqs))

    def test_no_git_url_returns_none(self):
        reqs = "scaler>=1.0\nboto3\n"
        self.assertIsNone(_extract_git_url_and_branch(reqs))

    def test_first_git_url_returned(self):
        reqs = "scaler @ git+https://github.com/org/repo.git@main\nother @ git+https://github.com/org/other.git@dev\n"
        self.assertEqual(_extract_git_url_and_branch(reqs), ("https://github.com/org/repo.git", "main"))
