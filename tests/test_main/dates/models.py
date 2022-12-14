from __future__ import unicode_literals

from django.db import models
# from django.utils.encoding import python_2_unicode_compatible


# @python_2_unicode_compatible
class Article(models.Model):
    title = models.CharField(max_length=100)
    pub_date = models.DateField()

    categories = models.ManyToManyField("Category", related_name="articles")

    def __str__(self):
        return self.title


# @python_2_unicode_compatible
class Comment(models.Model):
    article = models.ForeignKey(Article, related_name="comments", on_delete=models.CASCADE)
    text = models.TextField()
    pub_date = models.DateField()
    approval_date = models.DateField(null=True)

    def __str__(self):
        return 'Comment to %s (%s)' % (self.article.title, self.pub_date)


class Category(models.Model):
    name = models.CharField(max_length=255)
