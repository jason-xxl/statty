# -*- coding: utf-8 -*-

from django.contrib.auth.models import User
from django.db import models




"""



class FriendConversation(models.Model):
    A = models.ForeignKey(User, related_name='conversation_a')
    B = models.ForeignKey(User, related_name='conversation_b')
    last_message = models.ForeignKey('message.FriendMessage', related_name='conversation_last_message', null=True, blank=True)
    read_message = models.ForeignKey('message.FriendMessage', related_name='conversation_read_message', null=True, blank=True, help_text='Last read message')
    real_conversation = models.ForeignKey('message.FriendConversation', null=True, blank=True)
    updated_time = models.DateTimeField(null=True, blank=True)
    deleted = models.BooleanField(default=False)
    
    class Meta:
        db_table = u'friend_conversation'
        unique_together = ['A', 'B']
        
    def __unicode__(self):
        return u'Conversation between %s and %s' % (self.A.username, self.B.username)
    
class FriendMessage(models.Model):
    real_conversation = models.ForeignKey(FriendConversation)
    sender = models.ForeignKey(User, related_name='message_sender')
    recipient = models.ForeignKey(User, related_name='message_recipent')
    message = models.TextField(default='')
    sent_time = models.DateTimeField(auto_now_add=True)
    
    class Meta:
        db_table = u'friend_message'
        
    def __unicode__(self):
        return u'Message %d' % (self.id)
"""