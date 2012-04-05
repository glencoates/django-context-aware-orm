"""
All the classes necessary to provide models that are context-aware.
"""

import uuid
from collections import deque

from django.db import transaction, models

# ----------------------------------------------------------------------------------------------
# ContextAwareManager
# ----------------------------------------------------------------------------------------------

from django.db.models.manager import Manager
from django.db import router


class ContextAwareManager( Manager ):
    """
    A subclass of the core Manager that attempts to leverage caching to speed up ForeignKey lookups.
    """

    # Ensures that this manager is used for FK lookups
    use_for_related_fields = True

    # Override to compute the allowable PK sets for the target class
    def contribute_to_class( self, model, name ):

        super( ContextAwareManager, self ).contribute_to_class( model, name )

        self.pkAttNames = set( ["pk"] )
        self.parentEntityClasses = []
        queue = deque( model._meta.parents.items() )
        parentClass = None

        while queue:
            parentClass, toParentField = queue.popleft()
            self.pkAttNames.add( toParentField.name + "__pk" )
            self.parentEntityClasses.append( parentClass )
            queue.extend( parentClass._meta.parents.items() )

        # Include the final parent's classes own PK into the pkNames set
        if parentClass:
            self.pkAttNames.add( parentClass._meta.pk.name )

        # Get the parent entity types reading base -> subclasses in order
        self.parentEntityClasses.reverse()

        # Include the class's own PK into the pkNames set if it's there
        if hasattr( model._meta.pk, "name" ):
            self.pkAttNames.add( model._meta.pk.name )

        # Set up a pattern for detecting reverse lookups too
        self.oneToOneAttNames = dict( (f.name + "__pk", f) for f in model._meta.fields
                                      if f.rel and not f.rel.multiple and not f.rel.parent_link )


    def using( self, alias ):
        if alias == router.db_for_read( self.model ):
            return self
        else:
            return super( ContextAwareManager, self ).using( alias )


    def get( self, *args, **kw ):

        from . import ContextManager

        context = ContextManager.current( noneOnLookupFailure = True )

        # We are only able to use the cache when we have a single kwarg and it's a PK lookup
        if context and len( kw ) == 1:

            k, v = kw.items()[0]

            # Strip __exact from the end of the if it's there
            if k.endswith( "__exact" ):
                k = k[:-len( "__exact" )]

            # Determine if this is a UUID-based PK lookup
            isPK = isinstance( v, uuid.UUID ) and (k in self.pkAttNames)

            # OK let's try to hit the cache for this object's PK
            if isPK:
                fetchFunc = lambda: super( ContextAwareManager, self ).get( *args, **kw )
                return context.regdLookup( self.model.__name__, v, fetchFunc )

            # This is possibly a reverse lookup on a 1:1 relationship
            elif k in self.oneToOneAttNames:

                field = self.oneToOneAttNames[ k ]

                # In-memory spam through the candidates for this
                for _, obj in context.registeredEntities( self.model.__name__ ):
                    if getattr( obj, field.name + "_id", None ) == v:
                        return obj

        return super( ContextAwareManager, self ).get( *args, **kw )


    # Override to use an auto-registering QuerySet
    def get_query_set( self ):
        return RegisteringQuerySet( self.model, using = self._db )


# ----------------------------------------------------------------------------------------------
# RegisteringQuerySet
# ----------------------------------------------------------------------------------------------

from django.db.models.query import QuerySet

class RegisteringQuerySet( QuerySet ):
    """
    A QuerySet that registers all the instances that are fetched from the database.
    """

    def __init__( self, *args, **kw ):
        super( RegisteringQuerySet, self ).__init__( *args, **kw )

        from . import ContextManager
        self.context = ContextManager.current( noneOnLookupFailure = True )


    def iterator( self ):
        for result in super( RegisteringQuerySet, self ).iterator():
            if self.context:
                yield self.context.regd( result )
            else:
                yield result


    def registerParents( self ):
        """
        Register the parents of all the entities encapsulated by this queryset.
        """

        if self.context:
            self.context.registerParentEntities( self.model, list( self.iterator() ) )
            return self._clone()

        # If there is no context, no need to make a clone
        else:
            return self


# ----------------------------------------------------------------------------------------------
# ContextAwareModel
# ----------------------------------------------------------------------------------------------

from django.db.models.manager import Manager
from django.db.models.base import ModelBase

class ContextAwareModelBase( ModelBase ):
    """
    A subclass of the normal metaclass for models.  If it detects an attempt to create an instance
    of a model that PK collides with one that is already registered, it just returns the one from
    the registry.
    """

    def __call__( cls, *args, **kw ):

        # Make the new instance
        newborn = super( ContextAwareModelBase, cls ).__call__( *args, **kw )

        # If there is already a cached version of this instance, regd will return that instead.
        try:
            from . import ContextManager
            context = ContextManager.current()
            newborn._context = context
            cached = context.regd( newborn )

#             if id( cached ) != id( newborn ):
#                 mlog.ctx( context ).warning( "Returning cached instance for %s", cached )

            return cached

        # Just return the newly created instance if there is no context
        except KeyError:

            newborn._context = None
            import logging

            if cls.__name__ != "UserProfile":
                logging.warning( "Context aware model %s created outside of context", cls.__name__ )

            return newborn



class ContextAwareModel( models.Model ):
    """
    A subclass of the core Model that uses a ContextAwareManager to cache / speed up lookups.
    """

    __metaclass__ = ContextAwareModelBase
    objects = ContextAwareManager()

    class Meta:
        abstract = True


    @property
    def context( self ):
        """
        Returns the context for this model.
        """

        return getattr( self, "_context", None )


    def canBeRegd( self ):
        """
        Subclasses can override to indicate when it is actually safe to register them.
        """

        return self.pk is not None


    def save( self, *args, **kw ):
        """
        Try to register ourselves when we save.
        """

        ret = super( ContextAwareModel, self ).save( *args, **kw )

        from . import ContextManager
        context = ContextManager.current( noneOnLookupFailure = True )
        if context:
            context.regd( self )

        return ret
