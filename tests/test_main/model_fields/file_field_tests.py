# -*- coding: utf-8 -*-

import django
import os
import logging
os.environ.setdefault('DJANGO_SETTINGS_MODULE',
                      'tests.test_main.test_main.settings')
from black_mirror import api
os.environ.setdefault('WIKIBASE_URL', api('wikibase').url)
os.environ.setdefault('SPARQL_ENDPOINT', api('sparql').url)
logging.root.setLevel(logging.DEBUG)
logging.root.addHandler(logging.StreamHandler())

django.setup()

# End of section for predefined environment values

import os
import sys
import unittest

from django.core.files import temp
from django.core.files.uploadedfile import TemporaryUploadedFile
from django.db.utils import IntegrityError
from django.test import TestCase, override_settings

from tests.test_main.model_fields.models import Document


class FileFieldTests(TestCase):

    def test_clearable(self):
        """
        FileField.save_form_data() will clear its instance attribute value if
        passed False.
        """
        d = Document(myfile='something.txt')
        self.assertEqual(d.myfile, 'something.txt')
        field = d._meta.get_field('myfile')
        field.save_form_data(d, False)
        self.assertEqual(d.myfile, '')

    def test_unchanged(self):
        """
        FileField.save_form_data() considers None to mean "no change" rather
        than "clear".
        """
        d = Document(myfile='something.txt')
        self.assertEqual(d.myfile, 'something.txt')
        field = d._meta.get_field('myfile')
        field.save_form_data(d, None)
        self.assertEqual(d.myfile, 'something.txt')

    def test_changed(self):
        """
        FileField.save_form_data(), if passed a truthy value, updates its
        instance attribute.
        """
        d = Document(myfile='something.txt')
        self.assertEqual(d.myfile, 'something.txt')
        field = d._meta.get_field('myfile')
        field.save_form_data(d, 'else.jpg')
        self.assertEqual(d.myfile, 'else.jpg')

    def test_delete_when_file_unset(self):
        """
        Calling delete on an unset FileField should not call the file deletion
        process, but fail silently (#20660).
        """
        d = Document()
        d.myfile.delete()

    def test_refresh_from_db(self):
        d = Document.objects.create(myfile='something.txt')
        d.refresh_from_db()
        self.assertIs(d.myfile.instance, d)

    def test_defer(self):
        Document.objects.create(myfile='something.txt')
        self.assertEqual(Document.objects.defer('myfile')[0].myfile, 'something.txt')

    def test_unique_when_same_filename(self):
        """
        A FileField with unique=True shouldn't allow two instances with the
        same name to be saved.
        """
        Document.objects.create(myfile='something.txt')
        with self.assertRaises(IntegrityError):
            Document.objects.create(myfile='something.txt')

    @unittest.skipIf(sys.platform.startswith('win'), "Windows doesn't support moving open files.")
    # The file's source and destination must be on the same filesystem.
    @override_settings(MEDIA_ROOT=temp.gettempdir())
    def test_move_temporary_file(self):
        """
        The temporary uploaded file is moved rather than copied to the
        destination.
        """
        with TemporaryUploadedFile('something.txt', 'text/plain', 0, 'UTF-8') as tmp_file:
            tmp_file_path = tmp_file.temporary_file_path()
            Document.objects.create(myfile=tmp_file)
            self.assertFalse(os.path.exists(tmp_file_path), 'Temporary file still exists')
