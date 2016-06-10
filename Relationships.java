package nhs.genetics.cardiff.variantdatabase.plugin;

import org.neo4j.graphdb.RelationshipType;

/**
 * Relationship schema definition
 *
 * @author  Matt Lyon
 * @version 1.0
 * @since   2016-05-27
 */
class Relationships {
    static final RelationshipType hasData = RelationshipType.withName("HAS_DATA");
    static final RelationshipType hasEvent = RelationshipType.withName("HAS_EVENT");
    static final RelationshipType addedBy = RelationshipType.withName("ADDED_BY");
    static final RelationshipType authorisedBy = RelationshipType.withName("AUTHORISED_BY");
    static final RelationshipType rejectedBy = RelationshipType.withName("REJECTED_BY");
    static final RelationshipType designedBy = RelationshipType.withName("DESIGNED_BY");
    static final RelationshipType containsSymbol = RelationshipType.withName("CONTAINS_SYMBOL");
    static final RelationshipType hasHetVariant = RelationshipType.withName("HAS_HET_VARIANT");
    static final RelationshipType hasHomVariant = RelationshipType.withName("HAS_HOM_VARIANT");
    static final RelationshipType hasFeature = RelationshipType.withName("HAS_FEATURE");
    static final RelationshipType hasAssociatedSymbol = RelationshipType.withName("HAS_ASSOCIATED_SYMBOL");
    static final RelationshipType hasAnnotation = RelationshipType.withName("HAS_ANNOTATION");

    public static String getVariantInheritance(String inheritanceRelationshipTypeName){
        if (inheritanceRelationshipTypeName.length() > 12) {
            return inheritanceRelationshipTypeName.substring(4, inheritanceRelationshipTypeName.length() - 8);
        } else {
            return null;
        }
    }

}
