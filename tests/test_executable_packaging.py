import pathlib
import subprocess
import unittest

class ExecutablePackagingTest(unittest.TestCase):
    def test_documented_launchers_are_executable_in_git(self):
        root=pathlib.Path(__file__).resolve().parents[1]
        launchers=['mvnw','scripts/check.sh','scripts/run_checks.sh']
        entries=subprocess.check_output(['git','ls-files','--stage','--',*launchers],cwd=root,text=True).splitlines()
        modes={line.split('\t',1)[1]:line.split(' ',1)[0] for line in entries}
        self.assertEqual(set(launchers),set(modes))
        for name in launchers:
            with self.subTest(launcher=name):self.assertEqual('100755',modes[name],'A normal Linux clone must support direct execution')

if __name__=='__main__':unittest.main(verbosity=2)