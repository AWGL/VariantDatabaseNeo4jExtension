package nhs.genetics.cardiff.variantdatabase.plugin;

import org.neo4j.graphdb.Label;

/**
 * Label schema definition
 *
 * @author  Matt Lyon
 * @version 1.0
 * @since   2016-05-27
 */
class Labels {
    static final Label sample = Label.label("Sample");
    static final Label dataset = Label.label("Dataset");
    static final Label event = Label.label("Event");
    static final Label panel = Label.label("Panel");
    static final Label user = Label.label("User");
    static final Label symbol = Label.label("Symbol");
    static final Label variant = Label.label("Variant");
    static final Label pathogenicity = Label.label("Pathogenicity");
    static final Label annotate = Label.label("Annotate");
    static final Label queued = Label.label("Queued");
    static final Label feature = Label.label("Feature");
    static final Label canonical = Label.label("Canonical");
    static final Label disorder = Label.label("Disorder");
    static final Label qualityControl = Label.label("QualityControl");
    static final Label featurePreference = Label.label("FeaturePreference");
}
