# -*- coding: utf-8 -*-
# Generated by Django 1.11.29 on 2020-10-01 14:07
from __future__ import unicode_literals

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('stats', '0002_support_deleted_objects'),
    ]

    operations = [
        migrations.AlterField(
            model_name='consumer',
            name='consumerName',
            field=models.CharField(max_length=256, null=True),
        ),
        migrations.AlterUniqueTogether(
            name='subscription',
            unique_together=set([('name', 'topic', 'timestamp')]),
        ),
        migrations.AlterIndexTogether(
            name='subscription',
            index_together=set([]),
        ),
    ]
