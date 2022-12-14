# Django settings for test_main project.
from os import getenv

DEBUG = True
TEMPLATE_DEBUG = DEBUG


DATABASES = {
    'default': {
        'ENGINE': 'wikibase',
        'BOT_USERNAME': getenv('BOT_USERNAME', 'SYSDBA'),
        'BOT_PASSWORD': getenv('BOT_PASSWORD', 'MasterKey'),
        'URL': getenv('WIKIBASE_URL', 'https://phytonium.qstand.art'),
        'OPTIONS': {
            'charset': 'utf-8',
            'instance_of_property_id': 1,
            'subclass_of_property_id': 2,
            'wdqs_sparql_endpoint': getenv('SPARQL_ENDPOINT', 'https://phytonium.qstand.art/sparql'),
            'django_namespace': 'test_main'
        },

        'TEST': {
            'CHARSET': 'utf-8',
            'SERIALIZE': False,
            'PAGE_SIZE': 8192
        }
    },
    'other': {
        'ENGINE': 'wikibase',
        'BOT_USERNAME': getenv('BOT_USERNAME', 'SYSDBA'),
        'BOT_PASSWORD': getenv('BOT_PASSWORD', 'MasterKey'),
        'URL': getenv('WIKIBASE_URL', 'https://phytonium.qstand.art'),
        'OPTIONS': {
            'charset': 'utf-8',
            'instance_of_property_id': 1,
            'subclass_of_property_id': 2,
            'wdqs_sparql_endpoint': getenv('SPARQL_ENDPOINT', 'https://phytonium.qstand.art/sparql'),
            'django_namespace': 'test_main'
        },

        'TEST': {
            'CHARSET': 'utf-8',
            'SERIALIZE': False,
            'PAGE_SIZE': 8192
        }
    }

}

# Local time zone for this installation. Choices can be found here:
# http://en.wikipedia.org/wiki/List_of_tz_zones_by_name
# although not all choices may be available on all operating systems.
# In a Windows environment this must be set to your system time zone.
TIME_ZONE = 'America/Chicago'

# Language code for this installation. All choices can be found here:
# http://www.i18nguy.com/unicode/language-identifiers.html
LANGUAGE_CODE = 'en-us'

# If you set this to False, Django will make some optimizations so as not
# to load the internationalization machinery.
USE_I18N = True

# If you set this to False, Django will not format dates, numbers and
# calendars according to the current locale.
USE_L10N = True

# If you set this to False, Django will not use timezone-aware datetimes.
USE_TZ = False

# Absolute filesystem path to the directory that will hold user-uploaded files.
# Example: "/var/www/example.com/media/"
MEDIA_ROOT = ''

# URL that handles the media served from MEDIA_ROOT. Make sure to use a
# trailing slash.
# Examples: "http://example.com/media/", "http://media.example.com/"
MEDIA_URL = ''

# Absolute path to the directory static files should be collected to.
# Don't put anything in this directory yourself; store your static files
# in apps' "static/" subdirectories and in STATICFILES_DIRS.
# Example: "/var/www/example.com/static/"
STATIC_ROOT = ''

# URL prefix for static files.
# Example: "http://example.com/static/", "http://static.example.com/"
STATIC_URL = '/static/'

# Additional locations of static files
STATICFILES_DIRS = (
    # Put strings here, like "/home/html/static" or "C:/www/django/static".
    # Always use forward slashes, even on Windows.
    # Don't forget to use absolute paths, not relative paths.
)

# List of finder classes that know how to find static files in
# various locations.
STATICFILES_FINDERS = (
    'django.contrib.staticfiles.finders.FileSystemFinder',
    'django.contrib.staticfiles.finders.AppDirectoriesFinder',
    #    'django.contrib.staticfiles.finders.DefaultStorageFinder',
)

# Make this unique, and don't share it with anybody.
SECRET_KEY = ')!cccf)%m($bijkilb=z7-gjy_1!gj=v5^86(16%bl9fnr%ol0'

# List of callables that know how to import templates from various sources.
TEMPLATE_LOADERS = (
    'django.template.loaders.filesystem.Loader',
    'django.template.loaders.app_directories.Loader',
    #     'django.template.loaders.eggs.Loader',
)

MIDDLEWARE_CLASSES = (
    'django.middleware.common.CommonMiddleware',
    'django.contrib.sessions.middleware.SessionMiddleware',
    'django.middleware.csrf.CsrfViewMiddleware',
    'django.contrib.auth.middleware.AuthenticationMiddleware',
    'django.contrib.messages.middleware.MessageMiddleware',
    # Uncomment the next line for simple clickjacking protection:
    'django.middleware.clickjacking.XFrameOptionsMiddleware',
)

ROOT_URLCONF = 'test_main.urls'

# Python dotted path to the WSGI application used by Django's runserver.
WSGI_APPLICATION = 'test_main.wsgi.application'

TEMPLATE_DIRS = (
    # Put strings here, like "/home/html/django_templates" or "C:/www/django/templates".
    # Always use forward slashes, even on Windows.
    # Don't forget to use absolute paths, not relative paths.
)

INSTALLED_APPS = (
    'django.contrib.auth',
    'django.contrib.contenttypes',
    'django.contrib.sessions',
    'django.contrib.sites',
    'django.contrib.messages',
    'django.contrib.staticfiles',
    # Uncomment the next line to enable the admin:
    # 'django.contrib.admin',
    # Uncomment the next line to enable admin documentation:
    # 'django.contrib.admindocs',
    'tests.test_main.test_base',
    'tests.test_main.model_fields',
    'tests.test_main.many_to_one',
    'tests.test_main.transactions',
    'tests.test_main.schema',
    'tests.test_main.introspection',
    'tests.test_main.inspectdb',
    'tests.test_main.datatypes',
    'tests.test_main.dates',
    'tests.test_main.datetimes',
    'tests.test_main.lookup',
    'tests.test_main.expressions',
)

# A sample logging configuration. The only tangible logging
# performed by this configuration is to send an email to
# the site admins on every HTTP 500 error when DEBUG=False.
# See http://docs.djangoproject.com/en/dev/topics/logging for
# more details on how to customize your logging configuration.
LOGGING = {
    'version': 1,
    'disable_existing_loggers': False,
    'filters': {
        'require_debug_false': {
            '()': 'django.utils.log.RequireDebugFalse'
        }
    },
    'handlers': {
        'mail_admins': {
            'level': 'ERROR',
            'filters': ['require_debug_false'],
            'class': 'django.utils.log.AdminEmailHandler'
        }
    },
    'loggers': {
        'django.request': {
            'handlers': ['mail_admins'],
            'level': 'ERROR',
            'propagate': True,
        },
    }
}
