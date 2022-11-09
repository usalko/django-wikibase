Django-wikibase adapter
=======================

Yours settings.py sample:

.. code:: python

   DATABASES = {
       'default': {
           'ENGINE': 'wikibase',
           'BOT_USERNAME': getenv('BOT_USERNAME', 'SYSDBA'),    # Any user registerd as bot in mediawiki (with correct rights)
           'BOT_PASSWORD': getenv('BOT_PASSWORD', 'MasterKey'), # Bot password in mediawiki
           'URL': getenv('WIKIBASE_URL', 'http://localhost'),
           'OPTIONS': {
               'charset': 'utf-8',
               'instance_of_property_id': 1, # Change if instance of in local wikibase has another property identifier. For example: instance of has P2333 identity => 'instance_of_property_id': 2333
               'subclass_of_property_id': 2, # Change if instance of in local wikibase has another property identifier. For example: subclass of has P2777 identity => 'subclass_of_property_id': 2777 
               'wdqs_sparql_endpoint': getenv('SPARQL_ENDPOINT', 'http://localhost/sparql'),
               'django_namespace': 'my-cool-application-namespace'
           }
       }
   }

