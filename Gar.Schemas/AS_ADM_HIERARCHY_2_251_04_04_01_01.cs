﻿//------------------------------------------------------------------------------
// <auto-generated>
//     This code was generated by a tool.
//     Runtime Version:4.0.30319.42000
//
//     Changes to this file may cause incorrect behavior and will be lost if
//     the code is regenerated.
// </auto-generated>
//------------------------------------------------------------------------------

// 
// This source code was auto-generated by xsd, Version=4.8.3928.0.
// 
namespace AS_ADM_HIERARCHY_2_251_04_04_01_01.xsd {
    using System.Xml.Serialization;
    
    
    /// <remarks/>
    [System.CodeDom.Compiler.GeneratedCodeAttribute("xsd", "4.8.3928.0")]
    [System.SerializableAttribute()]
    [System.Diagnostics.DebuggerStepThroughAttribute()]
    [System.ComponentModel.DesignerCategoryAttribute("code")]
    [System.Xml.Serialization.XmlTypeAttribute(AnonymousType=true)]
    [System.Xml.Serialization.XmlRootAttribute(Namespace="", IsNullable=false)]
    public partial class ITEMS {
        
        private ITEMSITEM[] iTEMField;
        
        /// <remarks/>
        [System.Xml.Serialization.XmlElementAttribute("ITEM")]
        public ITEMSITEM[] ITEM {
            get {
                return this.iTEMField;
            }
            set {
                this.iTEMField = value;
            }
        }
    }
    
    /// <remarks/>
    [System.CodeDom.Compiler.GeneratedCodeAttribute("xsd", "4.8.3928.0")]
    [System.SerializableAttribute()]
    [System.Diagnostics.DebuggerStepThroughAttribute()]
    [System.ComponentModel.DesignerCategoryAttribute("code")]
    [System.Xml.Serialization.XmlTypeAttribute(AnonymousType=true)]
    public partial class ITEMSITEM {
        
        private long idField;
        
        private long oBJECTIDField;
        
        private long pARENTOBJIDField;
        
        private bool pARENTOBJIDFieldSpecified;
        
        private long cHANGEIDField;
        
        private string rEGIONCODEField;
        
        private string aREACODEField;
        
        private string cITYCODEField;
        
        private string pLACECODEField;
        
        private string pLANCODEField;
        
        private string sTREETCODEField;
        
        private long pREVIDField;
        
        private bool pREVIDFieldSpecified;
        
        private long nEXTIDField;
        
        private bool nEXTIDFieldSpecified;
        
        private System.DateTime uPDATEDATEField;
        
        private System.DateTime sTARTDATEField;
        
        private System.DateTime eNDDATEField;
        
        private ITEMSITEMISACTIVE iSACTIVEField;
        
        private string pATHField;
        
        /// <remarks/>
        [System.Xml.Serialization.XmlAttributeAttribute()]
        public long ID {
            get {
                return this.idField;
            }
            set {
                this.idField = value;
            }
        }
        
        /// <remarks/>
        [System.Xml.Serialization.XmlAttributeAttribute()]
        public long OBJECTID {
            get {
                return this.oBJECTIDField;
            }
            set {
                this.oBJECTIDField = value;
            }
        }
        
        /// <remarks/>
        [System.Xml.Serialization.XmlAttributeAttribute()]
        public long PARENTOBJID {
            get {
                return this.pARENTOBJIDField;
            }
            set {
                this.pARENTOBJIDField = value;
            }
        }
        
        /// <remarks/>
        [System.Xml.Serialization.XmlIgnoreAttribute()]
        public bool PARENTOBJIDSpecified {
            get {
                return this.pARENTOBJIDFieldSpecified;
            }
            set {
                this.pARENTOBJIDFieldSpecified = value;
            }
        }
        
        /// <remarks/>
        [System.Xml.Serialization.XmlAttributeAttribute()]
        public long CHANGEID {
            get {
                return this.cHANGEIDField;
            }
            set {
                this.cHANGEIDField = value;
            }
        }
        
        /// <remarks/>
        [System.Xml.Serialization.XmlAttributeAttribute()]
        public string REGIONCODE {
            get {
                return this.rEGIONCODEField;
            }
            set {
                this.rEGIONCODEField = value;
            }
        }
        
        /// <remarks/>
        [System.Xml.Serialization.XmlAttributeAttribute()]
        public string AREACODE {
            get {
                return this.aREACODEField;
            }
            set {
                this.aREACODEField = value;
            }
        }
        
        /// <remarks/>
        [System.Xml.Serialization.XmlAttributeAttribute()]
        public string CITYCODE {
            get {
                return this.cITYCODEField;
            }
            set {
                this.cITYCODEField = value;
            }
        }
        
        /// <remarks/>
        [System.Xml.Serialization.XmlAttributeAttribute()]
        public string PLACECODE {
            get {
                return this.pLACECODEField;
            }
            set {
                this.pLACECODEField = value;
            }
        }
        
        /// <remarks/>
        [System.Xml.Serialization.XmlAttributeAttribute()]
        public string PLANCODE {
            get {
                return this.pLANCODEField;
            }
            set {
                this.pLANCODEField = value;
            }
        }
        
        /// <remarks/>
        [System.Xml.Serialization.XmlAttributeAttribute()]
        public string STREETCODE {
            get {
                return this.sTREETCODEField;
            }
            set {
                this.sTREETCODEField = value;
            }
        }
        
        /// <remarks/>
        [System.Xml.Serialization.XmlAttributeAttribute()]
        public long PREVID {
            get {
                return this.pREVIDField;
            }
            set {
                this.pREVIDField = value;
            }
        }
        
        /// <remarks/>
        [System.Xml.Serialization.XmlIgnoreAttribute()]
        public bool PREVIDSpecified {
            get {
                return this.pREVIDFieldSpecified;
            }
            set {
                this.pREVIDFieldSpecified = value;
            }
        }
        
        /// <remarks/>
        [System.Xml.Serialization.XmlAttributeAttribute()]
        public long NEXTID {
            get {
                return this.nEXTIDField;
            }
            set {
                this.nEXTIDField = value;
            }
        }
        
        /// <remarks/>
        [System.Xml.Serialization.XmlIgnoreAttribute()]
        public bool NEXTIDSpecified {
            get {
                return this.nEXTIDFieldSpecified;
            }
            set {
                this.nEXTIDFieldSpecified = value;
            }
        }
        
        /// <remarks/>
        [System.Xml.Serialization.XmlAttributeAttribute(DataType="date")]
        public System.DateTime UPDATEDATE {
            get {
                return this.uPDATEDATEField;
            }
            set {
                this.uPDATEDATEField = value;
            }
        }
        
        /// <remarks/>
        [System.Xml.Serialization.XmlAttributeAttribute(DataType="date")]
        public System.DateTime STARTDATE {
            get {
                return this.sTARTDATEField;
            }
            set {
                this.sTARTDATEField = value;
            }
        }
        
        /// <remarks/>
        [System.Xml.Serialization.XmlAttributeAttribute(DataType="date")]
        public System.DateTime ENDDATE {
            get {
                return this.eNDDATEField;
            }
            set {
                this.eNDDATEField = value;
            }
        }
        
        /// <remarks/>
        [System.Xml.Serialization.XmlAttributeAttribute()]
        public ITEMSITEMISACTIVE ISACTIVE {
            get {
                return this.iSACTIVEField;
            }
            set {
                this.iSACTIVEField = value;
            }
        }
        
        /// <remarks/>
        [System.Xml.Serialization.XmlAttributeAttribute()]
        public string PATH {
            get {
                return this.pATHField;
            }
            set {
                this.pATHField = value;
            }
        }
    }
    
    /// <remarks/>
    [System.CodeDom.Compiler.GeneratedCodeAttribute("xsd", "4.8.3928.0")]
    [System.SerializableAttribute()]
    [System.Xml.Serialization.XmlTypeAttribute(AnonymousType=true)]
    public enum ITEMSITEMISACTIVE {
        
        /// <remarks/>
        [System.Xml.Serialization.XmlEnumAttribute("0")]
        Item0,
        
        /// <remarks/>
        [System.Xml.Serialization.XmlEnumAttribute("1")]
        Item1,
    }
}
