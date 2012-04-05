"""
An attempt to provide similar functionality to a NetSyncedObjectContext and the old SessionManager
object that was provided by the coredata module.
"""

import uuid
import threading
import weakref
import datetime
import traceback
from collections import deque

from django.db import transaction, models
from django.db.models import Q, ObjectDoesNotExist


# Helper function to return the current thread ID
def threadID():
    return threading.currentThread().name


class ContextManager( object ):

    # A mapping from thread ID -> instances of this class.  Used for doing static
    # lookups of the 'current' context.
    s_instances = weakref.WeakValueDictionary()

    # A mapping from request -> instances of this class.  Less horrible than the thread ID lookups,
    # with the general approach being that any controller that takes a request as an arg can do a
    # reliable lookup like this.
    s_instancesByRequest = weakref.WeakValueDictionary()

    # A lock for protecting access to the s_instances* dicts when absolutely required
    s_lock = threading.RLock()

    # A thread-local for mapping entity classes to stacks of their non-abstract parents
    s_concreteParents = threading.local()


    def __init__( self, request = None, registry = None ):

        # Write in basic attrs
        self.threadID = threadID()
        self.request = request

        # This is a dictionary of (typename, pk) -> model instance, created to allow you to
        # fetch an instance already registered with this context rather than getting a different
        # one referring to the same underlying DB object.
        # Until we get rid of netObjectIDs, we are also maintaining an ID registry.
        self.pkRegistry = {} # The keys are (type, pks)

        # Store a static refs to self indexed by thread and request
        with self.s_lock:
            self.s_instances[ self.threadID ] = self
            if self.request:
                self.s_instancesByRequest[ self.request ] = self

        # A savepoint stack to track the inner transactions of this manager
        self.savepoints = []

        # Pre-register any objects that were supplied
        if registry:
            for obj in registry:
                self.regd( obj )



    def __del__( self ):

        # Roll back any remaining savepoints
        if self.savepoints:

            mlog.ctx( self ).debug(
                "There were %d uncommitted savepoints for %s at __del__ time, rolling back",
                len( self.savepoints ), self.account.name )

            for sp in self.savepoints[::-1]:
                self.rollbackSavepoint( sp )


    def __str__( self ):
        return "%s (%s)" % (self.__class__.__name__, self.account.name)


    # Returns the context for the current thread
    @classmethod
    def current( cls, noneOnLookupFailure = False ):
        with cls.s_lock:
            if noneOnLookupFailure:
                return cls.s_instances.get( threadID(), None )
            else:
                return cls.s_instances[ threadID() ]


    # Look up the context for the given request
    @classmethod
    def forRequest( cls, request, noneOnLookupFailure = False ):
        with cls.s_lock:
            if noneOnLookupFailure:
                return cls.s_instancesByRequest.get( request, None )
            else:
                return cls.s_instancesByRequest[ request ]


    def parentClassnamesForEntity( self, entity ):
        """
        Returns a list of non-abstract parent classnames for a given meta type.
        """

        finalName = entity.__class__.__name__

        # Do parent stack setup if required
        if not hasattr( self.s_concreteParents, finalName ):

            stack = [finalName]
            setattr( self.s_concreteParents, finalName, stack )
            queue = deque( entity._meta.parents.keys() )

            while queue:
                parentClass = queue.popleft()
                stack.append( parentClass.__name__ )
                queue.extend( parentClass._meta.parents.keys() )

        return getattr( self.s_concreteParents, finalName )


    def regd( self, entity ):
        """
        A way of registering entities with this context that we might be worried that we'll access
        multiple times through different relationships or queries.  This helps avoid having two
        instances in-use that both refer to the same underlying DB object.
        """

        if hasattr( entity, "canBeRegd" ) and callable( entity.canBeRegd ):
            isRegisterable = entity.canBeRegd()
        else:
            isRegisterable = (entity is not None) and entity.pk

        if isRegisterable:

            from . import NetSyncedObject
            from context_aware_model import ContextAwareModel

            regkey = (entity.__class__.__name__, entity.pk)
            cached = self.pkRegistry.get( regkey, None )

            # Put it in the pkRegistry if it wasn't already in there
            if cached is None:

                # Register this entity for this specific class
                self.pkRegistry[ regkey ] = entity
                cached = entity

            return cached

        else:
            return entity


    def allRegd( self, entities ):
        """
        Iterator that registers entities before returning them
        """

        for e in entities:
            yield self.regd( e )


    def registerParentEntities( self, entityType, entities ):
        """
        Works around Django's frustrating refusal to (a) automatically select_related() for the
        parent_link and (b) to respect your wishes when you explicitly tell it to
        select_related( '_parent_nso' )
        """

        entityType = self.modelClass( entityType )
        pks = [e.pk for e in entities]

        for parentClass in entityType.objects.parentEntityClasses:
            for parentDupe in self.fetchEntities( parentClass, pk__in = pks ):
                self.regd( parentDupe )


    def regdLookup( self, entityType, pk, fetchFunc = None ):
        """
        A way to look up an object from the registry raw
        """

        # Cache hit
        try:
            return self.pkRegistry[ (entityType, pk) ]

        # Cache miss
        except KeyError:

            log = mlog.ctx( self )
            log.debug( "cache miss for %s %s", entityType, pk )

            # Attempt fetch if we can, and register on successful fetch
            if fetchFunc:
                ret = fetchFunc()
                if ret is not None:
                    log.debug( "Registered post-fetch %s %s %s", ret.__class__.__name__, ret.entityType, ret.pk )
                    return self.regd( ret )

            return None


    def registeredEntities( self, entityType ):
        """
        Generator for returning all registered entities of a given type.
        """

        for (t, pk), obj in self.pkRegistry.iteritems():
            if t == entityType:
                yield (pk, obj)


    def flushRegistry( self ):
        """
        Empty the registry.
        """

        self.pkRegistry = {}


    def ejectFromRegistry( self, obj ):
        """
        Flushes an object out of the registry.
        """

        from . import NetSyncedObject

        # Flush the PK registry
        key = (obj.__class__.__name__, obj.pk)

        if key in self.pkRegistry:
            del self.pkRegistry[ key ]


    def pushSavepoint( self ):
        """
        Pushes another master savepoint onto the stack for this context.
        """

        # Fire any queued saves that have already happened so they are excluded from the commit
        if self.hasQueuedSaves():
            self.fireQueuedSaves()

        sp = transaction.savepoint()
        self.savepoints.append( sp )

        return sp


    def commitSavepoint( self, savepoint ):
        """
        Commits previously set savepoint.
        """

        # Make sure we fire any queued saves before doing this to include them in the commit
        if self.hasQueuedSaves():
            self.fireQueuedSaves()

        if self.savepoints and (savepoint == self.savepoints[-1]):
            transaction.savepoint_commit( savepoint )
            self.savepoints.pop()
        else:
            raise ValueError, "You tried to commit a savepoint that wasn't at the top of the stack"


    def rollbackSavepoint( self, savepoint ):
        """
        Rollback previously set savepoint.
        """

        # Make sure we drop any queued saves before doing this so they don't survive the rollback
        if self.hasQueuedSaves():
            self.abortAllQueuedSaves()

        if self.savepoints and (savepoint == self.savepoints[-1]):
            transaction.savepoint_rollback( savepoint )
            self.savepoints.pop()
        else:
            raise ValueError, "You tried to commit a savepoint that wasn't at the top of the stack"


    def savepoint( self ):
        """
        Returns a Python context manager for executing statements inside a savepoint.
        Intended to be a replacement for transaction.commit_on_success() which acts on a local
        savepoint rather than the entire transaction.
        """

        class SavepointContext( object ):

            def __init__( self, mgr ):
                self.mgr = mgr
                self.savepoint = None
                self.managingTransaction = not transaction.is_managed()

            def __enter__( self ):
                if self.managingTransaction:
                    transaction.enter_transaction_management()
                    transaction.managed( True )
                self.savepoint = self.mgr.pushSavepoint()

            def __exit__( self, type, value, traceback ):
                try:
                    if type is None:
                        self.mgr.commitSavepoint( self.savepoint )
                        if self.managingTransaction:
                            transaction.commit()
                    else:
                        self.mgr.rollbackSavepoint( self.savepoint )
                        if self.managingTransaction:
                            transaction.rollback()
                finally:
                    if self.managingTransaction:
                        transaction.leave_transaction_management()


        return SavepointContext( self )


from django.dispatch import receiver
from django.db.models.signals import pre_delete

# Pre-delete signal to make sure that registered instances are flushed from the cache
@receiver( pre_delete, dispatch_uid = "ContextManager_pre_delete" )
def onPreDeleteSignal( **kw ):

    context = ContextManager.current( noneOnLookupFailure = True )

    if context:
        obj = kw[ "instance" ]
        context.ejectFromRegistry( obj )
